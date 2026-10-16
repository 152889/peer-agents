"""运行时：驱动 Agent 的事件循环，并为「开放通信」配上必要的护栏。

一旦放开子 Agent 之间的自由通信，就必然面对三个新问题 —— 这也是
任何对等多 Agent 系统都必须回答的工程问题：

  收敛性  —— 什么时候算「聊完了」？用 *静默检测*：整整一轮没有任何
             Agent 收到邮件，即视为达成稳定态。
  死循环  —— 消息去重（Bus 层）+ 追问一次即止（Brain 层）+ 跳数上限。
  可控性  —— 总消息预算上限，超限即熔断，避免烧掉 API 额度。

调度采用单线程轮转（round-robin）而非真并发，为的是让 trace 完全确定、
可复现。真实系统里换成 asyncio 或消息队列即可，Agent 逻辑无需改动。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .agent import Agent
from .blackboard import Blackboard
from .bus import MessageBus
from .message import CONTROL_KINDS, FINAL, Message


@dataclass
class RunResult:
    name: str
    rounds: int = 0
    messages: list[Message] = field(default_factory=list)
    dropped: list[tuple[Message, str, str]] = field(default_factory=list)
    duration_ms: float = 0.0
    final_report: str = ""
    fidelity: float = 0.0
    hit_budget: bool = False
    stats: dict = field(default_factory=dict)
    # 动态拓扑观测数据（仅 adaptive 模式有值）
    initial_edges: list[tuple[str, str]] | None = None
    final_edges: list[tuple[str, str]] | None = None
    edge_traffic: dict | None = None
    transit_hops: int = 0
    link_requests: int = 0
    link_accepts: int = 0
    link_rejects: int = 0
    evictions: int = 0
    avg_path_len: float = 0.0

    @property
    def total_messages(self) -> int:
        return len(self.messages)

    @property
    def control_messages(self) -> int:
        """控制面消息：建链协商。这是一次性投资，不该混进业务通信量里比。"""
        return sum(1 for m in self.messages if m.kind in CONTROL_KINDS)

    @property
    def data_messages(self) -> int:
        """数据面消息：真正承载协作内容的报文。"""
        return self.total_messages - self.control_messages

    @property
    def relay_touches(self) -> int:
        return sum(1 for m in self.messages if m.relay_to)

    @property
    def adaptive(self) -> bool:
        return self.final_edges is not None

    def hub_load(self, hub: str) -> int:
        """枢纽经手的**业务**消息条数：它发出、发给它、或替它代投的都算。

        这个数字可以近似看成主 Agent 的上下文开销 —— 中心化架构下，
        它约等于系统全部业务消息量；对等架构下，它只包含真正的「汇报」。
        """
        n = 0
        for m in self.messages:
            if m.kind in CONTROL_KINDS:
                continue
            if m.sender == hub or hub in m.recipients or m.relay_to or m.relayed_by == hub:
                n += 1
        return n

    def lateral(self, hub: str | None = None) -> int:
        """横向消息：子 Agent 之间不经枢纽的**业务**通信条数。"""
        n = 0
        for m in self.messages:
            if m.kind in CONTROL_KINDS:
                continue
            if hub is None:
                if len(m.recipients) <= 2 and "*" not in m.recipients:
                    n += 1
            elif m.sender != hub and hub not in m.recipients and not m.relay_to and m.relayed_by != hub:
                n += 1
        return n


class Runtime:
    def __init__(
        self,
        agents: list[Agent],
        bus: MessageBus,
        board: Blackboard,
        hub: str | None = None,
        max_rounds: int = 60,
        max_messages: int = 400,
    ) -> None:
        self.agents = agents
        self.bus = bus
        self.board = board
        self.hub = hub
        self.max_rounds = max_rounds
        self.max_messages = max_messages

    def run(self, name: str, ground_truth_tokens: list[str] | None = None) -> RunResult:
        t0 = time.perf_counter()
        result = RunResult(name=name)

        topo = self.bus.topology
        if topo is not None:
            result.initial_edges = topo.edges()

        for rnd in range(1, self.max_rounds + 1):
            result.rounds = rnd
            self.board.round = rnd   # 让大脑能判断信息的「新鲜度」
            # 先重试上一轮因不可达而挂起的消息（此时可能已协商出新的链路）
            self.bus.retry_pending()
            active = 0
            for agent in self.agents:
                # 唤醒条件有两个：信箱里有信，或者大脑手上还有没做完的事
                #（比如「问了没人答」正在等超时重试）。少了后者，那些消息
                # 会随着静默检测一起被埋掉。
                pending = getattr(agent.brain, "has_pending_work", None)
                if agent.inbox == 0 and not (pending and pending(self.board)):
                    continue
                active += 1
                for msg in agent.step(self.board):
                    # 交付物沉淀是**框架**的职责，不是大脑的职责。
                    # 大脑只决定「说什么」，框架决定「留下什么」。
                    # 否则 LLM 大脑（只发消息、不写黑板）的结论就会凭空消失。
                    if msg.kind == FINAL:
                        self.board.publish(msg.topic, msg.content, agent.id)
                    self.bus.send(msg)
            # 收敛判定必须同时看「没人有信」和「没有消息还卡在路上」——
            # 后者若被忽略，正在等建链的消息会在静默瞬间被悄悄遗弃。
            if active == 0 and not self.bus.pending:
                break
            if len(self.bus.audit) > self.max_messages:   # 熔断
                result.hit_budget = True
                break

        result.messages = list(self.bus.audit)
        result.dropped = list(self.bus.dropped)
        result.stats = self.bus.stats()
        result.duration_ms = (time.perf_counter() - t0) * 1000

        if topo is not None:
            result.final_edges = topo.edges()
            result.edge_traffic = dict(self.bus.edge_traffic)
            result.transit_hops = self.bus.transit_hops
            result.link_requests = self.bus.link_requests
            result.link_accepts = self.bus.link_accepts
            result.link_rejects = self.bus.link_rejects
            result.evictions = topo.evictions
            if self.bus.path_lengths:
                result.avg_path_len = sum(self.bus.path_lengths) / len(self.bus.path_lengths)

        report = self.board.artifacts.get("final_report", "")
        if not report:
            for topic, (content, _a) in self.board.facts.items():
                if "report" in topic:
                    report = content
                    break
        if not report:
            for _t, (content, _a) in self.board.facts.items():
                if "根因" in content:
                    report = content
        result.final_report = report
        if ground_truth_tokens:
            hit = sum(1 for tok in ground_truth_tokens if tok in report)
            result.fidelity = hit / len(ground_truth_tokens)
        return result
