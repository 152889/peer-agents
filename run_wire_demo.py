"""跨语言演示：把「发布平台」这个成员，换成一个用 Node 写的进程。

    python run_wire_demo.py
    python run_wire_demo.py --trace          # 把线上的每一帧都打出来
    python run_wire_demo.py --topology peer  # 换回静态对等拓扑

它做三件事，顺序很重要：

  1. 先跑一遍**全 Python** 的基线，记下结论；
  2. 再把 ``deploy`` 的大脑换成 ``tools/peer_agent.mjs`` 起的真进程，重跑一遍；
  3. **逐项比对**两次运行 —— 终报、轮次、消息量、保真度、拓扑演化。

第 3 步才是这个演示的重点。一个「跨语言能跑」的演示如果只打印一句"成功"，
那它什么也没证明。真正要回答的问题是：**换掉一个成员的语言之后，
这套系统的结论有没有被改动？** 如果有一项对不上，这里必须看得出来。

为什么挑 ``deploy``
-------------------
它不是随便挑的。``deploy`` 在场景里只做一件事：有人问它负责的
``release_meta``，它就照实回答 —— 没有追问、没有假设、不下结论。
而 Node 参考实现恰好就是这块能力（答所问，别无其他）。于是这两者
**行为等价**，替换才有可比性：差异只可能来自「线上走了一趟」，
不可能来自「干了不一样的事」。

换 ``dba`` 就不行 —— 它要顺着线索追问、要下根因结论，而参考实现
刻意不包含推理。拿一个只会答题的大脑去做一份需要推理的工作，
比出来的差异是决策差异，不是通信差异。那种比较没有意义。
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # 保证中文在 Windows 控制台正常输出

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from peer_agents import message as M  # noqa: E402
from peer_agents import transport as T  # noqa: E402
from peer_agents import wire as W  # noqa: E402
from peer_agents.remote import RemoteBrain  # noqa: E402
from peer_agents.scenario_incident import (  # noqa: E402
    GROUND_TRUTH_TOKENS,
    T_RELEASE,
    T_SCOPE,
    build,
)

NODE = shutil.which("node")
PEER_SCRIPT = ROOT / "tools" / "peer_agent.mjs"
HUB = "sre"
REMOTE_ID = "deploy"                       # 被换掉的那个成员
REMOTE_ROLE = "发布平台"
REMOTE_OWNS = (T_RELEASE,)
# 与 scenario_incident 里 deploy 的 facts 一字不差 —— 否则差异来自信息不同，不是来自语言不同
REMOTE_FACTS = {
    T_RELEASE: "v2.31 于昨日 14:00 由订单服务发布，本次发布不含任何 DDL 或索引变更。"
}

TRACE = "--trace" in sys.argv
TOPOLOGY = "adaptive"
for i, token in enumerate(sys.argv):
    if token == "--topology" and i + 1 < len(sys.argv):
        TOPOLOGY = sys.argv[i + 1]

FRAMES: list[tuple[str, dict]] = []


def tracer(direction: str, payload: dict) -> None:
    FRAMES.append((direction, payload))


def bootstrap(runtime) -> None:
    """与 run_demo.py 完全一致的开场 —— 两次运行的起点必须一样。"""
    runtime.bus.send(
        M.Message(
            sender=HUB,
            recipients=("gateway",),
            kind=M.TASK,
            topic=T_SCOPE,
            content="checkout 接口 P99 由 120ms 劣化到 900ms，请定位你这一侧是否有异常。",
        )
    )


def spawn_peer(server: T.WireServer) -> subprocess.Popen:
    cmd = [
        NODE or "node", str(PEER_SCRIPT),
        "--host", "127.0.0.1",
        "--port", str(server.port),
        "--agent", REMOTE_ID,
        "--role", REMOTE_ROLE,
        "--owns", ",".join(REMOTE_OWNS),
        "--decide-links",                    # 让远端自己决定接受哪些直连
    ]
    for topic, content in REMOTE_FACTS.items():
        cmd += ["--fact", f"{topic}={content}"]
    return subprocess.Popen(cmd, cwd=str(ROOT))


def brief(direction: str, payload: dict) -> str:
    """把一帧压成一行人能读的东西。"""
    kind = payload.get("t", "?")
    if kind == W.F_HELLO:
        return f"{kind:<8} agent={payload.get('agent')} owns={payload.get('owns')} caps={payload.get('caps')}"
    if kind == W.F_WELCOME:
        return f"{kind:<8} 名册={payload.get('agents')} 会话={payload.get('session')}"
    if kind == W.F_THINK:
        box = payload.get("box") or []
        detail = "、".join(f"{m['kind']}:{m['topic']}" for m in box) or "（空信箱）"
        return f"{kind:<8} 第 {payload.get('round')} 轮 · 收到 {detail}"
    if kind == W.F_SAY:
        msgs = payload.get("msgs") or []
        detail = "、".join(f"{m['kind']}:{m['topic']}→{','.join(m['to'])}" for m in msgs) or "（不说话）"
        return f"{kind:<8} {detail}"
    if kind == W.F_LINK_ASK:
        return f"{kind:<8} {payload.get('from')} 想跟我直连（为 {payload.get('topic')}）"
    if kind == W.F_LINK_ANSWER:
        return f"{kind:<8} {'接受' if payload.get('ok') else '拒绝'}"
    return f"{kind:<8}"


def main() -> int:
    if NODE is None:
        print("✗ 本机没有 node —— 这个演示的意义就是跨语言，所以不能降级跑。")
        print("  装一个 node（>=18）后重试；或看 docs/PROTOCOL.md 自己实现一个对端。")
        return 1

    print("=" * 78)
    print("跨语言通信演示 · 场景：checkout P99 劣化根因定位")
    print(f"把成员「{REMOTE_ID}（{REMOTE_ROLE}）」换成 {PEER_SCRIPT.name}，其余成员与场景一字未改")
    print("=" * 78)

    # ---- 1) 基线：全 Python -------------------------------------------------
    baseline_rt = build(topology=TOPOLOGY)
    bootstrap(baseline_rt)
    baseline = baseline_rt.run(name="全 Python", ground_truth_tokens=GROUND_TRUTH_TOKENS)

    # ---- 2) 换成远端大脑 ----------------------------------------------------
    server = T.WireServer(trace=tracer).listen()
    proc = spawn_peer(server)
    remote: RemoteBrain | None = None
    try:
        peer, hello = server.accept(timeout=15.0, expect_agent=REMOTE_ID)
        T.send_welcome(peer, session="wire-demo", agents=["sre", "gateway", REMOTE_ID,
                                                         "ordersvc"],
                       directory={T_RELEASE: [REMOTE_ID]}, hub=None)
        remote = RemoteBrain(peer, hello, session="wire-demo")

        wire_rt = build(topology=TOPOLOGY, brain_overrides={REMOTE_ID: remote})
        bootstrap(wire_rt)
        wired = wire_rt.run(name="换掉 deploy", ground_truth_tokens=GROUND_TRUTH_TOKENS)
    finally:
        if remote is not None:
            remote.hangup()
        try:
            code = proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            code = -1
        server.close()

    # ---- 3) 逐项比对 --------------------------------------------------------
    print()
    print(f"▶ A 组：全 Python（{baseline.rounds} 轮 / {baseline.data_messages} 条业务消息 / "
          f"保真度 {baseline.fidelity * 100:.0f}%）")
    print(f"   终报：{baseline.final_report}")
    print()
    print(f"▶ B 组：{REMOTE_ID} 换成 Node 进程（{wired.rounds} 轮 / {wired.data_messages} 条业务消息 / "
          f"保真度 {wired.fidelity * 100:.0f}%）")
    print(f"   终报：{wired.final_report}")

    checks = [
        ("终报文本完全一致", baseline.final_report == wired.final_report),
        ("协作轮次一致", baseline.rounds == wired.rounds),
        ("业务消息条数一致", baseline.data_messages == wired.data_messages),
        ("信息保真度一致", baseline.fidelity == wired.fidelity),
        ("两次都真的结案", baseline.completed and wired.completed),
        ("两次都没有故障", not baseline.faults and not wired.faults),
        ("拓扑演化结果一致", baseline.final_edges == wired.final_edges),
        ("消息序列逐条一致",
         [(m.sender, m.recipients, m.kind, m.topic, m.content) for m in baseline.messages]
         == [(m.sender, m.recipients, m.kind, m.topic, m.content) for m in wired.messages]),
    ]

    print()
    print("─" * 78)
    print("两次运行的比对（唯一变量：deploy 由 Python 实现换成了 Node 实现）")
    for label, ok in checks:
        print(f"  [{'✓' if ok else '✗'}] {label}")

    if wired.adaptive:
        print()
        print(f"  拓扑（{TOPOLOGY}）：{len(baseline.final_edges or [])} 条 vs "
              f"{len(wired.final_edges or [])} 条 · "
              f"建链 {wired.link_requests} 次（接受 {wired.link_accepts} / 拒绝 {wired.link_rejects}）")

    print()
    print("─" * 78)
    print(f"跨语言边界上实际发生的事：{len(FRAMES)} 帧，全部发生在同一台机器的环回接口上")
    by_kind: dict[str, int] = {}
    for _d, payload in FRAMES:
        by_kind[payload.get("t", "?")] = by_kind.get(payload.get("t", "?"), 0) + 1
    print("  " + " · ".join(f"{k} {v} 帧" for k, v in sorted(by_kind.items())))
    print(f"  远端大脑被问了 {remote.rounds if remote else 0} 次、"
          f"参与建链表态 {remote.link_asks if remote else 0} 次"
          f"（失败 {remote.link_errors if remote else 0} 次）")
    print(f"  Node 进程退出码：{code}（0 = 收到 bye 后自己走）")

    print()
    print("帧序列" + ("（--trace：含全部字段）" if TRACE else "（只列要点，加 --trace 看全字段）"))
    print("┄" * 78)
    for direction, payload in FRAMES:
        arrow = "宿主→远端" if direction == "->" else "远端→宿主"
        print(f"  {arrow}  {brief(direction, payload)}")
        if TRACE:
            print(f"           {json.dumps(payload, ensure_ascii=False)}")
    print("┄" * 78)

    ok = all(flag for _label, flag in checks)
    print()
    print("→ " + ("跨语言没有引入任何失真：结论、轮次、消息量与拓扑演化逐项一致。"
                  if ok else "存在差异，需要排查 —— 上面打 ✗ 的那几项就是线索。"))
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
