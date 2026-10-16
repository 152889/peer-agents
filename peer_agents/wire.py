"""跨语言线协议：让非 Python 写的大脑接入同一个通信框架。

为什么需要它
------------
在此之前，「Agent 之间的通信」其实是**进程内的对象传递** —— 把同一个
Python 对象塞进另一个 Python 列表。可这个项目研究的是 *通信*，而进程内
传递恰恰是通信问题里最简单的那一档：没有序列化、没有半包、没有对端消失、
没有版本不一致。

边界一旦挪到线上，这四件事会一起出现。本模块只做一件事：
**在这条线上重新划一遍权限边界。**

支点：远端就是「一个你不拥有的大脑」
------------------------------------
这是整个设计的立足点。远端进程**不接入总线、不碰拓扑、不写黑板** ——
它只实现 ``Brain.think`` 那一件事：看着信箱，说自己想发什么。

于是它继承的权限和 ``LLMBrain`` 完全一样，一行都不用放宽：
框架决定留下什么，大脑只决定说什么。远端想改拓扑，同样做不到。

字段三分法
----------
``WIRE_WRITABLE``   远端可以填：to / kind / topic / content / reply_to
``WIRE_FORBIDDEN``  填了就**整帧拒收**：sender / relayed_by / relay_to(见下) / origin_kind
``WIRE_IGNORED``    填了会被**静默覆盖**：id / hop / path / ts

为什么不写成「一律忽略」？因为这三类的失败方式不一样：

* **提权字段**若静默忽略，远端会以为自己成功了，然后对着一个永远不生效的
  行为查半天。这类「静止的失败」正是本项目 §9 想消灭的东西。更要命的是
  ``relayed_by``：§9 第 1 条已经证明，只要 ``relayed_by == hub``，报文就
  直接绕过拓扑投递 —— 让远端能填这个字段，等于给它开了一条拓扑旁路。
* **框架覆盖的字段**（id / hop / path）远端带上只是冗余 —— 比如它老老实实
  把收到的报文原样回传。拒收反而会误伤正常实现。

``relay_to`` 是个特例：它只在**对端恰好就是枢纽**时才允许出现，因为枢纽
代投是这个框架的既有语义（静态星型拓扑靠它复现「一切经中心转述」）。
即便允许，``relayed_by`` 也仍然由框架按对端身份**推**出来，绝不取自线上 ——
否则对端只要声称「我被枢纽转发过了」就能绕过拓扑。

报错策略：线上从严，不做静默降级
--------------------------------
LLM 边界的处理是「解析失败 → 这轮什么都不发」，因为模型输出本来就潦草，
丢掉一轮是优雅的。**线上不一样**：对端是有人照着规范写出来的程序，
它的输出不合规说明那个程序有 bug，而一个带原因的响亮报错才是作者需要的。

所以这里一律抛异常。抛出去之后由 Runtime 现有的故障隔离接住：原因进
``RunResult.faults``、在途报文进死信队列、连续出错达阈值就把这个成员摘除。
全是现成的机制，不必新造一条静默通道。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .message import CONTROL_KINDS, DATA_KINDS, Message

# 协议版本。对不上就明确拒绝，不做「尽力兼容」——
# 一个含糊兼容的协议，最后会变成每个人都只测自己那一半。
PROTO_VERSION = 1

# 单帧上限。防止对端用一个永不换行的巨行把我们撑爆。
# 和 ``MessageBus._seen`` 的滑动窗口是同一个道理：**无界的记忆就是泄漏**。
MAX_LINE_BYTES = 1 << 20      # 1 MiB
MAX_BATCH = 256               # 一帧最多携带的报文条数

# -- 帧类型 ----------------------------------------------------------------
F_HELLO = "hello"        # 对端 -> 宿主：我来了，这是我的身份与能力
F_WELCOME = "welcome"    # 宿主 -> 对端：欢迎，这是当前的名册与通讯录
F_THINK = "think"        # 宿主 -> 对端：这一轮你的信箱里有什么
F_SAY = "say"            # 对端 -> 宿主：这一轮我想发这些
F_LINK_ASK = "link?"     # 宿主 -> 对端：有人请求与你直连，你接受吗
F_LINK_ANSWER = "link!"  # 对端 -> 宿主：接受 / 拒绝
F_ERROR = "error"        # 宿主 -> 对端：你刚才那帧被拒了，原因是
F_BYE = "bye"            # 双向：收工

_FRAME_TYPES = frozenset(
    {F_HELLO, F_WELCOME, F_THINK, F_SAY, F_LINK_ASK, F_LINK_ANSWER, F_ERROR, F_BYE}
)

# -- 能力声明 --------------------------------------------------------------
# 对端可以在握手里声明「我愿意参与建链表态」。不声明就由框架按默认策略代答，
# 这和本地大脑用 ``getattr`` 探测 ``accepts_link`` 是否存在是同一套思路。
CAP_LINK = "accepts_link"
_KNOWN_CAPS = frozenset({CAP_LINK})

# -- 字段归属 --------------------------------------------------------------
WIRE_WRITABLE = frozenset({"to", "kind", "topic", "content", "reply_to"})
WIRE_FORBIDDEN = frozenset({"sender", "relayed_by", "origin_kind"})
WIRE_IGNORED = frozenset({"id", "msg_id", "hop", "path", "ts"})

# ``relay_to`` 单列：只有身份是枢纽的对端才准填
_CONDITIONAL = frozenset({"relay_to"})


# ---------------------------------------------------------------------------
# 异常：刻意只有一个根，方便调用方一把兜住
# ---------------------------------------------------------------------------
class WireError(Exception):
    """线协议层的一切失败。"""


class MalformedFrame(WireError):
    """对端发来的东西不是一帧合法报文。"""


class FrameTooLarge(WireError):
    """单帧超过 ``MAX_LINE_BYTES``。"""


class VersionMismatch(WireError):
    """协议版本对不上。"""


class PermissionDenied(WireError):
    """对端试图填写只有框架才能决定的字段。"""


# ---------------------------------------------------------------------------
# 握手
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Hello:
    """对端在握手时自报的身份。

    ``owns`` 是自报的职责范围，宿主拿它当**默认**的建链表态依据。这看着像
    给了对端一项权力，其实没有：本地大脑（``RuleBrain.accepts_link``）本来
    就可以随便表态，而链路预算与 LRU 淘汰在 ``Topology`` 里，不受表态影响。
    真正约束拓扑稠密程度的是预算，不是谁的意愿。
    """

    proto: int
    agent: str
    role: str = ""
    owns: tuple[str, ...] = ()
    caps: tuple[str, ...] = ()

    @property
    def can_decide_links(self) -> bool:
        return CAP_LINK in self.caps


def make_hello(agent: str, role: str = "", owns: tuple[str, ...] = (),
               caps: tuple[str, ...] = ()) -> dict:
    return {
        "t": F_HELLO,
        "proto": PROTO_VERSION,
        "agent": agent,
        "role": role,
        "owns": list(owns),
        "caps": list(caps),
    }


def parse_hello(payload: dict) -> Hello:
    proto = payload.get("proto")
    if not isinstance(proto, int) or isinstance(proto, bool):
        raise MalformedFrame("握手缺少整数字段 proto")
    if proto != PROTO_VERSION:
        raise VersionMismatch(
            f"协议版本不一致：对端 {proto}，本端 {PROTO_VERSION}。"
            "不做尽力兼容 —— 请让对端按 PROTOCOL.md 对齐版本号。"
        )
    agent = _require_str(payload, "agent")
    if not agent:
        raise MalformedFrame("握手字段 agent 不能为空")
    caps = _str_tuple(payload.get("caps"), "caps")
    unknown = [c for c in caps if c not in _KNOWN_CAPS]
    if unknown:
        # 不认识的能力不能默认开启 —— 那等于把「声明」变成「申请什么给什么」
        raise MalformedFrame(f"不可识别能力声明 {unknown}，本端支持 {sorted(_KNOWN_CAPS)}")
    return Hello(
        proto=proto,
        agent=agent,
        role=_optional_str(payload, "role") or "",
        owns=_str_tuple(payload.get("owns"), "owns"),
        caps=caps,
    )


def make_welcome(session: str, agents: list[str], directory: dict[str, list[str]],
                 hub: str | None = None) -> dict:
    return {
        "t": F_WELCOME,
        "proto": PROTO_VERSION,
        "session": session,
        "agents": list(agents),
        "directory": {k: list(v) for k, v in directory.items()},
        "hub": hub,
    }


# ---------------------------------------------------------------------------
# 编解码：NDJSON
# ---------------------------------------------------------------------------
def encode_frame(payload: dict) -> bytes:
    """把一帧编成一行 UTF-8 JSON（NDJSON）。

    为什么不用长度前缀的二进制帧：那种更省字节、少一次扫描，但**调试下限**
    差一截 —— 出问题时你没法用 ``nc`` 手搓一条消息发进去看反应。这个项目的
    读者是想弄清通信机制的人，可调试性比几个字节值钱。
    """
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return line.encode("utf-8") + b"\n"


def decode_line(raw: bytes) -> dict:
    """把一行（不含换行符）解成帧对象。任何不合规都抛 ``WireError`` 子类。"""
    if len(raw) > MAX_LINE_BYTES:
        raise FrameTooLarge(f"单帧 {len(raw)} 字节，超过上限 {MAX_LINE_BYTES}")
    try:
        text = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise MalformedFrame(f"不是合法 UTF-8：{exc}") from exc
    if not text:
        raise MalformedFrame("空帧")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MalformedFrame(f"不是合法 JSON：{exc}") from exc
    if not isinstance(obj, dict):
        raise MalformedFrame(f"帧必须是一个 JSON 对象，收到 {type(obj).__name__}")
    kind = obj.get("t")
    if not isinstance(kind, str):
        raise MalformedFrame("帧缺少字符串字段 t（帧类型）")
    if kind not in _FRAME_TYPES:
        raise MalformedFrame(f"不认识的帧类型 {kind!r}，本端支持 {sorted(_FRAME_TYPES)}")
    return obj


def make_error(reason: str) -> dict:
    """宿主拒收一帧时，**把原因告诉对端**再断开。

    不告而别会让人去猜是网络问题还是自己写错了 —— 而这恰恰是本项目
    最想消除的体验。
    """
    return {"t": F_ERROR, "reason": reason}


def make_bye() -> dict:
    return {"t": F_BYE}


# ---------------------------------------------------------------------------
# 出站：框架 -> 线上
# ---------------------------------------------------------------------------
def message_to_wire(msg: Message) -> dict:
    """把一条 Message 摊平成线格式。

    带上 ``id`` 是因为对端需要它才能 ``reply_to`` —— 寻址信息必须跟着消息走，
    这是去中心化通信能成立的前提（见 ``message.py`` 的注释）。
    带上 ``relay_to`` 是因为对端可能就是枢纽，得知道自己该代投什么。
    """
    out: dict = {
        "id": msg.msg_id,
        "from": msg.sender,
        "to": list(msg.recipients),
        "kind": msg.kind,
        "topic": msg.topic,
        "content": msg.content,
        "reply_to": msg.reply_to,
    }
    if msg.relay_to:
        out["relay_to"] = msg.relay_to
    return out


def make_think(round_no: int, me_id: str, me_role: str, box: list[Message],
               directory: dict[str, list[str]], facts: dict[str, str]) -> dict:
    """宿主每轮问对端一次：「你现在想发什么？」"""
    return {
        "t": F_THINK,
        "round": round_no,
        "me": {"id": me_id, "role": me_role},
        "box": [message_to_wire(m) for m in box],
        # 共享黑板的只读视图：让远端也能用「通讯录 + 看板」这条间接通道
        "directory": {k: list(v) for k, v in directory.items()},
        "facts": dict(facts),
    }


# ---------------------------------------------------------------------------
# 入站：线上 -> 框架（**权限边界所在**）
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SayResult:
    """一帧 ``say`` 解出来的全部内容。"""

    speeches: tuple[Speech, ...] = ()
    # 对端自己的「未了之事」声明。缺了它，远端 Agent 就不可能在「问了没人答」
    # 时被重新唤醒 —— §9 第 7 条那次事故（系统在重试窗口打开之前就宣布收敛）
    # 会原样在线协议上重演一遍。
    pending: bool = False


@dataclass(frozen=True)
class Speech:
    """远端大脑说的「一句话」，已经过白名单消毒。

    它还没有 ``msg_id``、没有 ``sender`` —— 那两样由框架发。这是刻意的：
    远端不该有能力决定自己是「谁」，也不该有能力决定自己在 trace 里的位置。
    """

    recipients: tuple[str, ...]
    kind: str
    topic: str
    content: str
    reply_to: str | None = None
    relay_to: str | None = None
    relayed_by: str | None = None

    def to_message(self, sender: str) -> Message:
        return Message(
            sender=sender,
            recipients=self.recipients,
            kind=self.kind,
            topic=self.topic,
            content=self.content,
            reply_to=self.reply_to,
            relay_to=self.relay_to,
            relayed_by=self.relayed_by,
        )


def parse_say(payload: dict, *, peer_id: str, known_agents: frozenset[str] | set[str],
              hub: str | None) -> SayResult:
    """校验并消毒一帧 ``say``。

    这是**唯一的入站闸门**。它做四件事：
      1. 结构校验（``to`` 必须是字符串列表、``content`` 必须是字符串）；
      2. 权限校验（提权字段一律拒帧）；
      3. ``kind`` 必须落在数据面白名单内；
      4. 身份绑定（``sender`` 由宿主按会话身份填，不听对端的）。

    第 1 条对应 §9 第 11 条：``tuple("audit")`` 会炸成五个幽灵收件人
    ``a/u/d/i/t``，而真正该收到消息的那个一个字节都没拿到 —— 无异常、
    无日志，只表现为「他们没回我」。所以这里**绝不**对字符串做序列化解释。

    第 3 条对应 §9 第 12 条：模型（现在还有远端进程）若能发控制面报文，
    就能伪造建链请求、绕开框架改拓扑。总线的 ``send()`` 还会再拦一次 ——
    权限不能只押在一处。
    """
    items = payload.get("msgs")
    if not isinstance(items, list):
        raise MalformedFrame("say 帧缺少列表字段 msgs")
    if len(items) > MAX_BATCH:
        raise MalformedFrame(f"单帧 {len(items)} 条报文，超过上限 {MAX_BATCH}")

    pending = payload.get("pending", False)
    if not isinstance(pending, bool):
        raise MalformedFrame("say 帧的 pending 必须是布尔值")

    out: list[Speech] = []
    for idx, item in enumerate(items):
        out.append(_parse_one(item, idx, peer_id=peer_id, known_agents=known_agents, hub=hub))
    return SayResult(speeches=tuple(out), pending=pending)


def _parse_one(item: object, idx: int, *, peer_id: str,
               known_agents: frozenset[str] | set[str], hub: str | None) -> Speech:
    where = f"msgs[{idx}]"
    if not isinstance(item, dict):
        raise MalformedFrame(f"{where} 必须是 JSON 对象，收到 {type(item).__name__}")

    # -- 1) 提权字段：填了就拒帧 ------------------------------------------
    for field in sorted(WIRE_FORBIDDEN):
        if field in item:
            raise PermissionDenied(
                f"{where} 试图填写框架保留字段 {field!r}。"
                "远端只能决定「说什么」，不能决定「我是谁」「我被谁转过」。"
            )

    # -- 2) relay_to：只有枢纽身份的对端才准填 -----------------------------
    relay_to = _optional_str(item, "relay_to")
    if relay_to is not None:
        if peer_id != hub:
            raise PermissionDenied(
                f"{where} 试图代投（relay_to={relay_to!r}），但对端 {peer_id!r} 不是枢纽"
                f"（当前枢纽为 {hub!r}）。代投会绕过拓扑，只有枢纽有权这么做。"
            )
        if relay_to not in known_agents:
            raise MalformedFrame(f"{where} 的 relay_to={relay_to!r} 不在名册里")

    # -- 3) 收件人：必须已经是列表，绝不替字符串猜意图 ---------------------
    raw_to = item.get("to")
    if not isinstance(raw_to, list):
        raise MalformedFrame(
            f"{where} 的 to 必须是字符串数组，收到 {type(raw_to).__name__}。"
            "写成单个字符串会被误解成一串单字收件人（见 README §9 第 11 条），"
            "所以这里直接拒收，而不是替你猜。"
        )
    if not raw_to:
        raise MalformedFrame(f"{where} 的 to 为空 —— 公告请显式写 [\"*\"]")
    recipients: list[str] = []
    for r in raw_to:
        if not isinstance(r, str) or not r:
            raise MalformedFrame(f"{where} 的 to 含非法收件人 {r!r}")
        recipients.append(r)
    if len(set(recipients)) != len(recipients):
        raise MalformedFrame(f"{where} 的 to 含重复收件人 {recipients}")

    # -- 4) kind：数据面白名单 --------------------------------------------
    kind = item.get("kind")
    if not isinstance(kind, str):
        raise MalformedFrame(f"{where} 缺少字符串字段 kind")
    if kind in CONTROL_KINDS:
        raise PermissionDenied(
            f"{where} 试图发送控制面报文 {kind!r}。控制面只能由总线自己生成 —— "
            "否则对端就能伪造建链请求、绕开框架改写拓扑。"
        )
    if kind not in DATA_KINDS:
        raise MalformedFrame(f"{where} 的 kind={kind!r} 不在数据面白名单 {sorted(DATA_KINDS)} 内")

    # -- 5) 正文与寻址 ----------------------------------------------------
    content = item.get("content")
    if not isinstance(content, str):
        raise MalformedFrame(f"{where} 缺少字符串字段 content")
    topic = _require_str(item, "topic")
    if not topic:
        raise MalformedFrame(f"{where} 的 topic 不能为空")

    # relayed_by 由**身份**推出，绝不取自线上（见模块 docstring）
    relayed_by = peer_id if relay_to is not None else None

    return Speech(
        recipients=tuple(recipients),
        kind=kind,
        topic=topic,
        content=content,
        reply_to=_optional_str(item, "reply_to"),
        relay_to=relay_to,
        relayed_by=relayed_by,
    )


# ---------------------------------------------------------------------------
# 建链表态
# ---------------------------------------------------------------------------
def make_link_ask(requester: str, topic: str, origin_kind: str) -> dict:
    return {"t": F_LINK_ASK, "from": requester, "topic": topic, "origin_kind": origin_kind}


def parse_link_answer(payload: dict) -> bool:
    ok = payload.get("ok")
    if not isinstance(ok, bool):
        raise MalformedFrame("link! 帧缺少布尔字段 ok")
    return ok


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------
def _require_str(payload: dict, key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise MalformedFrame(f"字段 {key!r} 必须是字符串，收到 {type(value).__name__}")
    return value


def _optional_str(payload: dict, key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise MalformedFrame(f"字段 {key!r} 必须是字符串或 null，收到 {type(value).__name__}")
    return value


def _str_tuple(value: object, key: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise MalformedFrame(f"字段 {key!r} 必须是字符串数组，收到 {type(value).__name__}")
    for item in value:
        if not isinstance(item, str):
            raise MalformedFrame(f"字段 {key!r} 含非字符串元素 {item!r}")
    return tuple(value)
