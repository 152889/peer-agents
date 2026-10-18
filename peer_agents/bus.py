"""消息总线：整个系统唯一的「通信基础设施」。

它就是一个内存版的消息中间件，职责有四：
  1. 投递：把 Message 放进每个收件人的信箱（mailbox）。
  2. 拓扑管控：决定「谁有权直接跟谁说话」。支持两种模式 ——
     * **静态策略** ``policy(src, dst)``：开局写死，用于对照实验；
     * **动态拓扑** ``topology``：链路是运行时状态，由 Agent 自主协商增删，
       无直连时沿最短路径多跳转发。
  3. 建链协商：拓扑模式下，消息无路可走或路径过长时，总线替发送方发起
     ``LINK_REQUEST``，由收方的大脑表态是否接受（``accept_hook``）。
  4. 审计：记录每一条消息与其实际走过的路径，供事后分析通信量、拓扑演化
     和信息损耗。

控制面 / 数据面分离
-------------------
建链协商走**带外寻址**（直接按地址投递，不受当前拓扑约束），
业务消息则严格受拓扑约束。对应真实网络里路由协议与数据转发的分离。

中转（relay）机制
-----------------
静态星型拓扑下，若 policy 禁止 A 直发 B，总线不静默丢包，而是改投给
``hub`` 并带上 ``relay_to=B``，由 hub 代投。这样「一切经主 Agent 转述」
不需要任何业务代码改动就能复现 —— 这也是公平对照实验的前提。
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable

from .message import CONTROL_KINDS, LINK_ACCEPT, LINK_REJECT, LINK_REQUEST, Message
from .topology import Key, Topology, edge_key


class MessageBus:
    def __init__(
        self,
        policy: Callable[[str, str], bool] | None = None,
        hub: str | None = None,
        topology: Topology | None = None,
        accept_hook: Callable[[str, str, Message], bool] | None = None,
        max_hop: int = 40,
        dedup: bool = True,
        max_link_attempts: int = 4,
        dedup_window: int = 4096,
    ) -> None:
        self.boxes: dict[str, list[Message]] = {}
        self.audit: list[Message] = []
        self.dropped: list[tuple[Message, str, str]] = []
        self.policy = policy
        self.hub = hub
        self.topology = topology
        self.accept_hook = accept_hook
        self.max_hop = max_hop
        self.dedup = dedup
        self.max_link_attempts = max_link_attempts
        # 去重窗口：只保留最近 N 条签名。
        # 去重要拦的是「绕圈」—— 绕圈总是在很短的窗口内发生，所以滑动窗口
        # 完全够用；而无限累积下去，长跑时这张表会一直涨（实测 5000 条消息
        # 就常驻 5000 条记录）。这是用「有限记忆」换「不泄漏」。
        self.dedup_window = dedup_window

        self._seen: OrderedDict[tuple, None] = OrderedDict()
        self._link_pending: set[Key] = set()

        # 观测数据
        self.pending: list[tuple[Message, int]] = []
        self.edge_traffic: dict[Key, int] = {}
        self.path_lengths: list[int] = []
        self.transit_hops = 0
        self.link_requests = 0
        self.link_accepts = 0
        self.link_rejects = 0

    # -- 接线 -------------------------------------------------------------
    def register(self, *agent_ids: str) -> None:
        for aid in agent_ids:
            self.boxes.setdefault(aid, [])

    def take(self, agent_id: str) -> list[Message]:
        msgs = self.boxes.get(agent_id, [])
        self.boxes[agent_id] = []
        return msgs

    def mail_count(self, agent_id: str) -> int:
        return len(self.boxes.get(agent_id, []))

    def allowed(self, src: str, dst: str) -> bool:
        if self.policy is None:
            return True
        return bool(self.policy(src, dst))

    # -- 投递 -------------------------------------------------------------
    def send(self, msg: Message, attempts: int = 0) -> int:
        # 权限边界：控制面报文只能由总线自己生成。外部的 send() 一律拒收。
        #
        # 少了这道闸，任何大脑（尤其是 LLM 大脑）只要吐出
        # {"kind": "LINK_REQUEST"} 就能伪造建链请求、直接改写拓扑 ——
        # 所谓「模型只能决定跟谁说什么，不能改动框架」就成了空话。
        if msg.kind in CONTROL_KINDS:
            self.dropped.append((msg, "*", "control-plane-not-from-agent"))
            return 0

        if self.dedup:
            sig = msg.signature()
            if sig in self._seen:
                self.dropped.append((msg, "*", "duplicate"))
                return 0

        delivered = 0
        pended = False
        for dst in self._resolve(msg):
            if dst == msg.sender:
                continue
            if dst not in self.boxes:
                self.dropped.append((msg, dst, "unknown-agent"))
                continue
            if msg.hop >= self.max_hop:
                self.dropped.append((msg, dst, "hop-limit"))
                continue
            n, parked = self._transmit(msg, dst, attempts)
            delivered += n
            pended = pended or parked

        if self.dedup and delivered and not pended:
            self._remember(msg.signature())
        return delivered

    def _remember(self, sig: tuple) -> None:
        """记住一条已投递的签名，并把窗口外的旧签名挤出去。"""
        self._seen[sig] = None
        self._seen.move_to_end(sig)
        while len(self._seen) > self.dedup_window:
            self._seen.popitem(last=False)

    def _resolve(self, msg: Message) -> list[str]:
        out: list[str] = []
        for r in msg.recipients:
            if r == "*":
                out.extend(a for a in self.boxes if a != msg.sender)
            else:
                out.append(r)
        return list(dict.fromkeys(out))

    def _transmit(self, msg: Message, dst: str, attempts: int) -> tuple[int, bool]:
        """返回（投递条数，是否被挂起等待建链）。"""
        # 注意 hub 可能为 None，必须先判空：否则 `None == None` 恒真，
        # 会让所有消息绕过拓扑直投。
        if self.hub is not None and msg.relayed_by == self.hub:
            self._deliver(msg, dst)
            return 1, False
        if self.topology is not None:
            return self._transmit_routed(msg, dst, attempts)
        if self.allowed(msg.sender, dst):
            self._deliver(msg, dst)
            return 1, False
        if self.hub and self.hub in self.boxes:
            # 拓扑不允许直连 —— 交给枢纽代投，而不是丢弃
            self._deliver(msg.clone(recipients=(self.hub,), relay_to=dst), self.hub)
            return 1, False
        self.dropped.append((msg, dst, "blocked-by-topology"))
        return 0, False

    # -- 动态拓扑下的路由 --------------------------------------------------
    def _transmit_routed(self, msg: Message, dst: str, attempts: int) -> tuple[int, bool]:
        assert self.topology is not None
        topo = self.topology
        path = topo.route(msg.sender, dst)

        if path and len(path) >= 2:
            for i in range(len(path) - 1):
                a, b = path[i], path[i + 1]
                topo.touch(a, b)
                k = edge_key(a, b)
                self.edge_traffic[k] = self.edge_traffic.get(k, 0) + 1
            self.transit_hops += len(path) - 2
            self.path_lengths.append(len(path) - 1)
            self._deliver(msg.clone(hop=msg.hop + len(path) - 1, path=tuple(path)), dst)
            # 路径过长说明这条通信需求是长期的 —— 主动争取一条直连
            if len(path) > 2:
                self._request_link(msg.sender, dst, msg)
            return 1, False

        # 无路可走：挂起消息，同时发起建链协商
        self.pending.append((msg, attempts + 1))
        self._request_link(msg.sender, dst, msg)
        return 0, True

    # -- 建链协商 ----------------------------------------------------------
    def _request_link(self, a: str, b: str, msg: Message) -> None:
        topo = self.topology
        if topo is None or self.accept_hook is None or a == b:
            return
        if topo.has_link(a, b) or (a, b) in self._link_pending:
            return
        self._link_pending.add((a, b))
        req = Message(
            sender=a,
            recipients=(b,),
            kind=LINK_REQUEST,
            topic=msg.topic,
            content=f"{a} 请求与 {b} 建立直连（通信需求：{msg.topic}）",
            origin_kind=msg.kind,   # 让对方知道这是在替哪种业务消息谈链路
        )
        self._deliver_control(req, b)  # 控制面走带外寻址

    def _deliver_control(self, msg: Message, dst: str) -> None:
        m = msg.clone(hop=msg.hop + 1)
        self.audit.append(m)
        if m.kind != LINK_REQUEST:
            return

        # 用提前返回而不是 assert：assert 在 `python -O` 下会被整个剥掉，
        # 把一条本该优雅退场的分支变成「生产环境才炸」的隐藏地雷。
        if self.topology is None:
            return
        self.link_requests += 1
        accepted = bool(self.accept_hook and self.accept_hook(dst, m.sender, m))
        linked = accepted and self.topology.try_link(m.sender, dst)
        if linked:
            self.link_accepts += 1
        else:
            self.link_rejects += 1
            self._link_pending.discard((m.sender, dst))
        self.audit.append(
            Message(
                sender=dst,
                recipients=(m.sender,),
                kind=LINK_ACCEPT if linked else LINK_REJECT,
                topic=m.topic,
                content=f"{dst} {'接受' if linked else '拒绝'}了 {m.sender} 的直连请求",
                hop=m.hop + 1,
            )
        )

    # -- 重试挂起的消息 ----------------------------------------------------
    def retry_pending(self) -> int:
        """每轮开跑前重试一次挂起的消息（此时可能已经协商出新的链路）。"""
        items, self.pending = self.pending, []
        delivered = 0
        for msg, att in items:
            if att > self.max_link_attempts:
                self.dropped.append((msg, "*", "unreachable-after-negotiation"))
                continue
            delivered += self.send(msg, attempts=att)
        return delivered

    # -- 落箱 -------------------------------------------------------------
    def _deliver(self, msg: Message, dst: str) -> None:
        if msg.kind in CONTROL_KINDS:
            self._deliver_control(msg, dst)
            return
        m = msg.clone(hop=msg.hop + 1)
        self.boxes[dst].append(m)
        self.audit.append(m)

    # -- 统计 -------------------------------------------------------------
    def stats(self) -> dict:
        sent: dict[str, int] = dict.fromkeys(self.boxes, 0)
        recv: dict[str, int] = dict.fromkeys(self.boxes, 0)
        for m in self.audit:
            sent[m.sender] = sent.get(m.sender, 0) + 1
            for r in self._resolve(m):
                recv[r] = recv.get(r, 0) + 1
        return {"sent": sent, "recv": recv, "total": len(self.audit)}


def full_mesh_policy(_src: str, _dst: str) -> bool:
    """对等拓扑：任意两个 Agent 都能直连。"""
    return True


def star_policy(hub: str) -> Callable[[str, str], bool]:
    """星型拓扑：只有枢纽能直连他人，其余必须经枢纽转述。"""

    def _policy(src: str, dst: str) -> bool:
        return src == hub or dst == hub

    return _policy
