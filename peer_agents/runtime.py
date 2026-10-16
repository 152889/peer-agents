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
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field

from .agent import Agent
from .blackboard import Blackboard
from .bus import MessageBus
from .message import CONTROL_KINDS, FINAL, Message, reset_ids

# 退出原因：一次实验是怎么停下来的
QUIESCENT = "quiescent"      # 全员静默，判定收敛
BUDGET = "budget"            # 触发消息熔断
MAX_ROUNDS = "max-rounds"    # 耗尽轮次上限

EXIT_LABEL = {
    QUIESCENT: "全员静默，判定收敛",
    BUDGET: "触发消息熔断",
    MAX_ROUNDS: "耗尽轮次上限",
}


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
    # ---- 结果判读：这次到底是「结案了」还是「悄悄失败了」 --------------
    exit_reason: str = ""                     # 见上方 QUIESCENT / BUDGET / MAX_ROUNDS
    completed: bool = False                   # 是否真的产出了终态交付物
    unresolved: dict[str, list[str]] = field(default_factory=dict)  # Agent -> 还缺什么
    faults: list[dict] = field(default_factory=list)                # 故障隔离记录
    dead_letters: list[tuple[str, Message]] = field(default_factory=list)
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
    def ok(self) -> bool:
        """这次实验算不算一次成功的运行。

        注意 **「没结案」≠「没跑完」**。一个安静的、没有任何报错的空结果，
        正是最需要被看见的那种失败 —— 它会长得和「任务本来就没东西可做」
        一模一样。
        """
        return self.completed and not self.faults

    def describe_outcome(self) -> str:
        """把「到底成没成」说成一句人能直接读的话。

        没有它，调用方只能靠 `final_report == ""` 去猜，而一个空字符串
        既可能是「失败」，也可能只是「还没跑到那儿」。
        """
        head = "[✓] 已结案" if self.completed else "[✗] 未结案"
        where = EXIT_LABEL.get(self.exit_reason, self.exit_reason or "未知")
        lines = [f"{head} —— {where}（{self.rounds} 轮，{self.data_messages} 条业务消息）"]
        if not self.completed:
            if self.unresolved:
                for aid, items in sorted(self.unresolved.items()):
                    lines.extend(f"      缺 {aid}：{it}" for it in items)
            else:
                lines.append("      没有 Agent 报告未满足的前提，但也没有产出终态交付物")
        if self.faults:
            who = "；".join(f"{f['agent']} {f['error']}" for f in self.faults[:3])
            more = "…" if len(self.faults) > 3 else ""
            lines.append(f"      隔离故障 {len(self.faults)} 次：{who}{more}")
        if self.dead_letters:
            lines.append(f"      死信 {len(self.dead_letters)} 条：在途消息随故障一起未处理")
        return "\n".join(lines)

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
        max_faults_per_agent: int = 3,
    ) -> None:
        self.agents = agents
        self.bus = bus
        self.board = board
        self.hub = hub
        self.max_rounds = max_rounds
        self.max_messages = max_messages
        # 连续故障到这个次数，就把该成员摘除 —— 让它不再拖住整个会话
        self.max_faults_per_agent = max_faults_per_agent
        self._ran = False
        # 构造一个 Runtime = 一次新实验的开始，消息编号从这里归零。
        #
        # 放在这里而不是 run() 里，是因为开场消息（kickoff）发生在 run() 之前 ——
        # 若在 run() 开头才归零，两次运行的**第一条消息**编号就会不同，
        # trace 也就永远 diff 不了。归零点必须早于这个实验产生的任何一条消息。
        reset_ids()

    # ------------------------------------------------------------------
    def _record_fault(self, agent: Agent, batch: list[Message], exc: Exception,
                      result: RunResult) -> None:
        """一个成员崩了：记下来、把在途消息送进死信队列、然后继续跑。

        没有这一层，任何一个 Agent 的异常都会让整轮协作直接中断 ——
        而在一个自主通信的群体里，「某个成员此刻不可用」是常态，不是意外。

        注意在途的那批消息进了**死信队列**而不是被丢掉：它们已经从信箱里
        取出来了，如果只是继续往下走，就等于凭空蒸发 —— 而「消息凭空消失」
        正是这个项目里最难查的一类 bug。

        它被三处调用，因为一个成员能出错的地方有三处（见各自注释）：
        ``think``、``has_pending_work``、以及它产出的消息进入总线的那一刻。
        """
        agent.faults += 1
        agent.last_error = f"{type(exc).__name__}: {exc}"
        result.dead_letters.extend((agent.id, m) for m in batch)
        result.faults.append({
            "agent": agent.id,
            "error": agent.last_error,
            "round": self.board.round,
            "lost_messages": len(batch),
            "traceback": traceback.format_exc(limit=4),
        })
        if agent.faults >= self.max_faults_per_agent:
            agent.alive = False

    def _brain_says_busy(self, agent: Agent, fn: Callable[[Blackboard], object] | None,
                         result: RunResult) -> bool:
        """问大脑「你手上还有没做完的事吗」。

        这一步原本裸露在故障隔离之外，是 §9 第 22 条记下的那个缺口：
        ``has_pending_work`` 一旦抛异常，异常会一路穿过整个 ``run()``，
        把这个会话掀翻 —— 而它和 ``think`` 一样，都是大脑提供的、由框架
        调用的方法。凭什么只保其中一个？

        出错时返回 ``False`` 并记一次故障：把它当成「没有未了之事」继续跑，
        总好过整轮崩掉；而这次故障会出现在 ``RunResult.faults`` 里，
        所以它并没有被吞掉。
        """
        if fn is None:
            return False
        try:
            return bool(fn(self.board))
        except Exception as exc:              # noqa: BLE001 —— 隔离边界，必须兜住一切
            self._record_fault(agent, [], exc, result)
            return False

    def _on_bus_fault(self, exc: Exception, result: RunResult) -> None:
        """总线自己在重试阶段的异常 —— 没有具体的责任 Agent，但同样必须可见。"""
        result.faults.append({
            "agent": "*",
            "error": f"{type(exc).__name__}: {exc}",
            "round": self.board.round,
            "lost_messages": 0,
            "traceback": traceback.format_exc(limit=4),
        })

    def _collect_unresolved(self) -> dict[str, list[str]]:
        """没结案时，挨个问大脑「你还缺什么」。

        这是「失败可见」的另一半：光说「失败」没用，得说清卡在哪个环节 ——
        是没人知道、问了没回、还是拿到的版本不可信。三者的处置方式完全不同。
        """
        out: dict[str, list[str]] = {}
        for agent in self.agents:
            fn = getattr(agent.brain, "blockers", None)
            if fn is None:
                continue
            try:
                items = fn(agent, self.board)
            except Exception:      # noqa: BLE001 —— 连「报缺什么」都失败时，不能再掀翻整轮
                continue
            if items:
                out[agent.id] = list(items)
        return out

    # ------------------------------------------------------------------
    def run(self, name: str, ground_truth_tokens: list[str] | None = None) -> RunResult:
        # 一次实验只能跑一次。复用同一个 Runtime 跑第二遍时，大脑、黑板、
        # 总线、拓扑全都是脏的 —— 实测会返回「轮次 1、消息数照抄」的结果：
        # 看起来成功，其实一条消息都没真正流动（全被去重吃掉了）。
        # 这种**静默的假成功**比直接报错危险得多，所以宁可在这里拦住。
        if self._ran:
            raise RuntimeError(
                "同一个 Runtime 只能运行一次：Agent 的大脑、黑板与总线都是有状态的，"
                "复用会得到一个看似成功、实则什么都没做的结果。请重新 build() 一个新实例。"
            )
        self._ran = True
        t0 = time.perf_counter()
        result = RunResult(name=name)

        topo = self.bus.topology
        if topo is not None:
            result.initial_edges = topo.edges()

        for rnd in range(1, self.max_rounds + 1):
            result.rounds = rnd
            self.board.round = rnd   # 让大脑能判断信息的「新鲜度」
            # 先重试上一轮因不可达而挂起的消息（此时可能已协商出新的链路）。
            # 重试会走一遍完整的投递路径，也就会碰到大脑的建链表态 ——
            # 所以它同样在故障隔离范围内。
            try:
                self.bus.retry_pending()
            except Exception as exc:            # noqa: BLE001 —— 隔离边界，必须兜住一切
                self._on_bus_fault(exc, result)
            active = 0
            for agent in self.agents:
                if not agent.alive:
                    continue
                # 唤醒条件有两个：信箱里有信，或者大脑手上还有没做完的事
                #（比如「问了没人答」正在等超时重试）。少了后者，那些消息
                # 会随着静默检测一起被埋掉。
                pending = getattr(agent.brain, "has_pending_work", None)
                if agent.inbox == 0 and not self._brain_says_busy(agent, pending, result):
                    continue
                # 取信与处理分开：崩溃时调度器才知道该把哪一批消息送进死信队列
                batch = agent.bus.take(agent.id)
                try:
                    produced = agent.process(batch, self.board)
                except Exception as exc:            # noqa: BLE001 —— 隔离边界，必须兜住一切
                    self._record_fault(agent, batch, exc, result)
                    # 出错的成员仍在尝试，所以它依然算「活跃」——
                    # 否则一次瞬时故障会让系统误判收敛，提前收工。
                    active += 1
                    continue
                agent.faults = 0        # 连续计数：一次成功就清零
                active += 1
                for msg in produced:
                    # 交付物沉淀是**框架**的职责，不是大脑的职责。
                    # 大脑只决定「说什么」，框架决定「留下什么」。
                    # 否则 LLM 大脑（只发消息、不写黑板）的结论就会凭空消失。
                    if msg.kind == FINAL:
                        self.board.publish(msg.topic, msg.content, agent.id)
                        result.completed = True
                    # 投递过程会回调大脑的建链表态（accepts_link），所以它也在
                    # 故障隔离范围内。把这条消息当作「在途」记入死信队列 ——
                    # 它确实已经产出了，不该在抛异常时凭空消失。
                    try:
                        self.bus.send(msg)
                    except Exception as exc:        # noqa: BLE001 —— 隔离边界，必须兜住一切
                        self._record_fault(agent, [msg], exc, result)
            # 收敛判定必须同时看「没人有信」和「没有消息还卡在路上」——
            # 后者若被忽略，正在等建链的消息会在静默瞬间被悄悄遗弃。
            if active == 0 and not self.bus.pending:
                result.exit_reason = QUIESCENT
                break
            if len(self.bus.audit) > self.max_messages:   # 熔断
                result.hit_budget = True
                result.exit_reason = BUDGET
                break
        else:
            # 循环自然走完 = 从未触发收敛，是被轮次上限截断的
            result.exit_reason = MAX_ROUNDS

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
            for content, _author in self.board.facts.values():
                if "根因" in content:
                    report = content
        result.final_report = report
        if ground_truth_tokens:
            hit = sum(1 for tok in ground_truth_tokens if tok in report)
            result.fidelity = hit / len(ground_truth_tokens)

        # 收尾：没结案就把原因问清楚
        if not result.completed:
            result.unresolved = self._collect_unresolved()
        return result
