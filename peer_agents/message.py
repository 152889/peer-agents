"""消息模型：Agent 之间传递的一切都是一种 Message。

设计要点
--------
1. 一条消息可以同时发给多个收件人（``recipients`` 是 tuple），
   收件人为 ``"*"`` 表示广播（发布/订阅语义）。
2. 消息是**不可变**的（frozen dataclass）。转发、截断、改收件人一律用
   ``dataclasses.replace`` 生成新对象，避免多 Agent 共享可变状态。
3. ``reply_to`` 让答复可以沿着原路返回，不需要收件人自己维护会话表 ——
   这是去中心化通信能成立的关键：**寻址信息跟着消息走**。
4. ``relay_to`` 是「被拓扑挡住、改由中转站代投」的标记；``relayed_by``
   标记这条消息已经由中转站投递过，总线据此放行，避免死循环。
"""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field, replace

# 消息编号：**确定性递增**，而不是 uuid4。
#
# 为了让两次同配置的运行能逐字节 diff —— 这是「可复现」这个词的实际含义。
# 单线程确定性调度下，构造顺序固定，于是编号也固定；`Runtime.run()` 开头会
# 调 `reset_ids()`，保证同一进程内跑第二遍也是同一个序列。
_id_seq = itertools.count()


def reset_ids() -> None:
    """把编号序列拨回零。由 `Runtime.run()` 在每次实验开始时调用。"""
    global _id_seq
    _id_seq = itertools.count()


def _next_id() -> str:
    return f"m{next(_id_seq):05d}"

# 语义化消息类型。用 kind 而不是裸文本，方便总线做路由与审计。
TASK = "TASK"        # 上级/协作者派活
QUERY = "QUERY"      # 向某个 Agent 索要信息
ANSWER = "ANSWER"    # 应答 QUERY
FINDING = "FINDING"  # 主动广播结论
FINAL = "FINAL"      # 终态交付
INFORM = "INFORM"    # 单向通知

# 控制面消息：不由 Agent 大脑处理，由网络层直接消费
LINK_REQUEST = "LINK_REQUEST"
LINK_ACCEPT = "LINK_ACCEPT"
LINK_REJECT = "LINK_REJECT"
LINK_REVOKE = "LINK_REVOKE"

CONTROL_KINDS = frozenset({LINK_REQUEST, LINK_ACCEPT, LINK_REJECT, LINK_REVOKE})

# 数据面消息：**Agent 的大脑**可以自由选择的那几种。
# 这张白名单是权限边界 —— 控制面报文只能由总线自己生成，
# 一旦让大脑（尤其是 LLM 大脑）也能发，拓扑就成了模型可以随手改的东西。
DATA_KINDS = frozenset({TASK, QUERY, ANSWER, FINDING, FINAL, INFORM})


@dataclass(frozen=True)
class Message:
    sender: str
    recipients: tuple[str, ...]
    kind: str
    topic: str
    content: str
    msg_id: str = field(default_factory=_next_id)
    reply_to: str | None = None
    hop: int = 0
    attempt: int = 0             # 第几次尝试（0 = 首次）。重试是「新的一次发送」
    relay_to: str | None = None
    relayed_by: str | None = None
    path: tuple[str, ...] = ()   # 本次投递实际穿过的节点序列（多跳路由留痕）
    origin_kind: str = ""        # 控制面报文用来携带「它在替哪种业务消息谈链路」
    ts: float = field(default_factory=time.time)   # 墙上时钟，仅供观测，不参与任何判定

    # -- 便捷构造 ---------------------------------------------------------
    def clone(self, **kw) -> Message:
        return replace(self, **kw)

    def to(self, *recipients: str) -> Message:
        return replace(self, recipients=tuple(recipients))

    # -- 展示 -------------------------------------------------------------
    @property
    def targets(self) -> str:
        return "全体" if "*" in self.recipients else "/".join(self.recipients)

    def brief(self) -> str:
        tag = "转发 " if self.relay_to else ""
        return f"{self.sender} --[{self.kind}:{self.topic}]--> {tag}{self.targets}"

    def signature(self) -> tuple:
        """用于去重：同一发送者、同一批收件人、同一语义内容只投递一次。

        注意必须把 ``relayed_by`` 纳入签名：枢纽代投出来的报文，其
        sender/recipients/kind/topic/content 与原始报文完全一致，若不加这个
        维度就会被去重逻辑误判为重复投递而丢弃，中转链会直接断掉。

        同理必须纳入 ``attempt``：一个「问了没人答、过一会儿再问」的重试，
        正文一字不差，可它是**一次全新的发送**，不是同一条消息绕了回来。
        去重要拦的是「绕圈」，不是「再试一次」。
        """
        return (
            self.sender,
            self.recipients,
            self.kind,
            self.topic,
            self.content,
            self.relayed_by,
            self.attempt,
        )
