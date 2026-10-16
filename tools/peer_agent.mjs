#!/usr/bin/env node
/**
 * peer-agents 线协议 v1 —— Node 参考实现（零 npm 依赖）
 *
 * 它的存在只有一个理由：**证明这份协议能被另一种语言照着实现出来**。
 * 一份只有 Python 实现的"跨语言协议"，和一份只有作者本人能读的规范，
 * 是同一回事。
 *
 * 它实现的是一个最小可用的大脑：答所问、别无其他。
 * 对应 Python 侧的 ``RuleBrain`` 的 R1 规则 —— 刻意不做 R2/R3，
 * 因为参考实现的目标是"跑通协议"，不是"复刻推理"。
 *
 * 用法：
 *   node tools/peer_agent.mjs --port 8912 --agent dba --role 数据库团队 \
 *        --owns slow_query --fact slow_query="order_items 全表扫描" --decide-links
 *
 * 退出码：
 *   0  正常收工（收到 bye 或对端断开）
 *   2  参数错误
 *   3  协议违规（对端发来了不合规的帧）
 *   4  连不上宿主
 */

import net from 'node:net';

const PROTO_VERSION = 1;
const MAX_LINE_BYTES = 1 << 20; // 与 wire.py 的 MAX_LINE_BYTES 保持一致

const EXIT_ARG = 2;
const EXIT_PROTOCOL = 3;
const EXIT_CONNECT = 4;

// ---------------------------------------------------------------------------
// 参数
// ---------------------------------------------------------------------------
function fail(code, msg) {
  process.stderr.write(`[peer_agent] ${msg}\n`);
  process.exit(code);
}

function parseArgs(argv) {
  const out = { fact: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--open') { out.open = true; continue; }
    if (token === '--decide-links') { out.decideLinks = true; continue; }
    if (token === '--verbose') { out.verbose = true; continue; }
    if (token.startsWith('--')) {
      const key = token.slice(2);
      const value = argv[i + 1];
      if (value === undefined || value.startsWith('--')) {
        fail(EXIT_ARG, `参数 ${token} 缺少取值`);
      }
      i += 1;
      if (key === 'fact') out.fact.push(value);
      else out[key] = value;
      continue;
    }
    fail(EXIT_ARG, `不认识的参数 ${token}`);
  }
  return out;
}

const args = parseArgs(process.argv.slice(2));
if (!args.port) fail(EXIT_ARG, '必须提供 --port');
if (!args.agent) fail(EXIT_ARG, '必须提供 --agent');

const host = args.host || '127.0.0.1';
const port = Number(args.port);
const agentId = args.agent;
const role = args.role || agentId;

// 我负责的领域 —— 与握手时自报的 owns 是同一份
const owns = new Set((args.owns || '').split(',').map((s) => s.trim()).filter(Boolean));

// 我手上的私有事实：唯一的一手信息来源
const facts = new Map();
for (const item of args.fact) {
  const at = item.indexOf('=');
  if (at <= 0) fail(EXIT_ARG, `--fact 必须是 topic=内容 的形式，收到 ${item}`);
  facts.set(item.slice(0, at), item.slice(at + 1));
}

if (args.verbose) process.stderr.write(`[${agentId}] 连 ${host}:${port}，负责 ${[...owns].join('/') || '(无)'}\n`);

// ---------------------------------------------------------------------------
// 收发：行分隔 JSON，与 PROTOCOL.md 一一对应
// ---------------------------------------------------------------------------
const sock = net.createConnection({ host, port });
sock.setEncoding('utf8');
sock.setNoDelay(true);

let buf = '';
const answered = new Set(); // 同一个 topic 只答一次（RuleBrain 的 _answered）
let closing = false;

function send(frame) {
  if (closing) return;
  if (args.verbose) process.stderr.write(`[${agentId}] -> ${JSON.stringify(frame)}\n`);
  sock.write(`${JSON.stringify(frame)}\n`);
}

sock.on('connect', () => {
  send({
    t: 'hello',
    proto: PROTO_VERSION,
    agent: agentId,
    role,
    owns: [...owns],
    // 能力要显式声明才生效 —— 不声明的话，宿主会按默认策略代答建链表态
    caps: args.decideLinks ? ['accepts_link'] : [],
  });
});

sock.on('data', (chunk) => {
  buf += chunk;
  if (buf.length > MAX_LINE_BYTES && buf.indexOf('\n') < 0) {
    fail(EXIT_PROTOCOL, `累计 ${buf.length} 字节仍未见到换行，超过单帧上限`);
  }
  let nl = buf.indexOf('\n');
  while (nl >= 0) {
    const line = buf.slice(0, nl);
    buf = buf.slice(nl + 1);
    if (line.trim()) dispatch(line);
    nl = buf.indexOf('\n');
  }
});

sock.on('error', (err) => fail(EXIT_CONNECT, `连接出错：${err.message}`));
sock.on('close', () => process.exit(0));

// ---------------------------------------------------------------------------
// 帧处理
// ---------------------------------------------------------------------------
function dispatch(line) {
  let frame;
  try {
    frame = JSON.parse(line);
  } catch (err) {
    fail(EXIT_PROTOCOL, `收到不是合法 JSON 的一行：${err.message}`);
    return;
  }
  if (args.verbose) process.stderr.write(`[${agentId}] <- ${JSON.stringify(frame)}\n`);

  switch (frame.t) {
    case 'welcome':
      if (frame.proto !== PROTO_VERSION) {
        fail(EXIT_PROTOCOL, `协议版本不一致：宿主 ${frame.proto}，本端 ${PROTO_VERSION}`);
      }
      if (args.verbose) {
        process.stderr.write(`[${agentId}] 名册：${(frame.agents || []).join(', ')}\n`);
      }
      break;

    case 'think':
      send({ t: 'say', msgs: think(frame), pending: false });
      break;

    case 'link?':
      send({ t: 'link!', ok: acceptLink(frame) });
      break;

    case 'error':
      // 宿主明确说了为什么拒收我们 —— 原样报出来，别自己去猜
      fail(EXIT_PROTOCOL, `宿主拒收：${frame.reason}`);
      break;

    case 'bye':
      closing = true;
      sock.end();
      break;

    default:
      fail(EXIT_PROTOCOL, `不认识的帧类型 ${JSON.stringify(frame.t)}`);
  }
}

/**
 * 看着信箱，说自己想发什么。
 *
 * 注意「想发什么」的口径：只能填 to / kind / topic / content / reply_to。
 * 填 id / sender / relay_to 一律被宿主拒收 —— 我是大脑，不是框架。
 */
function think(frame) {
  const out = [];
  for (const msg of frame.box || []) {
    // 我是枢纽时替被拓扑挡住的消息代投（relay_to 只在宿主认定我是枢纽时才会出现）
    if (msg.relay_to) {
      out.push({
        to: [msg.relay_to],
        kind: msg.kind,
        topic: msg.topic,
        content: msg.content,
        reply_to: msg.reply_to ?? null,
      });
      continue;
    }
    if (msg.kind !== 'TASK' && msg.kind !== 'QUERY') continue;
    if (!owns.has(msg.topic)) continue;
    if (answered.has(msg.topic)) continue;
    answered.add(msg.topic);
    out.push({
      to: [msg.from],
      kind: 'ANSWER',
      topic: msg.topic,
      content: facts.get(msg.topic) ?? '（无可用信息）',
      reply_to: msg.id ?? null,
    });
  }
  return out;
}

/** 有人请求与我直连 —— 链路是稀缺资源，得有明确的接受标准。 */
function acceptLink(frame) {
  if (args.open) return true;
  if (owns.has(frame.topic)) return true;
  // 对方在答我、或在向我汇报：接上，否则结论回不来
  if (frame.origin_kind === 'ANSWER' || frame.origin_kind === 'FINAL') return true;
  return false;
}
