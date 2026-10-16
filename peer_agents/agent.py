"""Agent 与「大脑」。

Agent = 身份 + 信箱 + 一块可插拔的大脑（Brain）。
把认知逻辑抽成 Brain，是为了让同一个通信框架既能跑确定性的
规则大脑（用于可复现的演示与压测），也能换成真实的 LLM 大脑。

RuleBrain 是一个极简的「触发式」推理机，三条规则：
  R1 答所问   —— 问题落在我的职责范围，就回答。
  R2 顺藤摸瓜 —— 一旦得知 X，而我推导下去还需要 Y，就主动去问 Y 的负责人。
                 （这条是「子 Agent 互相交流」的价值所在：知识链在 Agent 之间
                  横向流动，不必回到枢纽再分发。）
  R3 达成结论 —— 当假设所需的全部前提都到手，输出结论并上报。
"""

from __future__ import annotations

from .blackboard import Blackboard
from .bus import MessageBus
from .message import ANSWER, FINAL, FINDING, INFORM, QUERY, TASK, Message


class Agent:
    def __init__(self, agent_id: str, role: str, brain: "RuleBrain | object", bus: MessageBus) -> None:
        self.id = agent_id
        self.role = role
        self.brain = brain
        self.bus = bus
        self.notes: dict[str, str] = {}  # 我已知的 topic -> 内容
        self.done: bool = False
        bus.register(agent_id)

    @property
    def inbox(self) -> int:
        return self.bus.mail_count(self.id)

    def step(self, board: Blackboard) -> list[Message]:
        """轮转一次。

        即便信箱是空的，也要把「空批次」交给大脑 —— 因为大脑手上可能有
        与收信无关的未了之事，最典型的就是「问了没人答，过一会儿要重试」。

        调度器只在「有信」或「大脑自认还有事」时才轮到这里（见 Runtime），
        所以这次调用不会让空闲 Agent 空转。
        """
        batch = self.bus.take(self.id)
        return self.brain.think(self, batch, board)

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Agent {self.id} role={self.role!r}>"


class RuleBrain:
    def __init__(
        self,
        owns: tuple[str, ...] = (),
        facts: dict[str, str] | None = None,
        triggers: tuple[tuple[str, str], ...] = (),
        hypothesis: tuple[set[str], str, str] | None = None,
        report_to: str | None = None,
        summarize_chars: int | None = None,
    ) -> None:
        self.owns = set(owns)
        self.facts = dict(facts or {})
        self.triggers = list(triggers)      # [(已知 topic, 需要的 topic), ...]
        self.hypothesis = hypothesis        # (前提 topic 集合, 结论 topic, 结论模板)
        self.report_to = report_to
        # 仅枢纽会用到：代投时把长消息截断，模拟「主 Agent 转述时丢细节」
        self.summarize_chars = summarize_chars
        # 是否愿意跟「非本领域」的 Agent 建立直连（默认只接本领域的活儿）
        self.open_link = False
        self._asked: set[str] = set()
        self._answered: set[str] = set()

    # ------------------------------------------------------------------
    def accepts_link(self, requester: str, msg: Message) -> bool:
        """是否接受来自 ``requester`` 的直连请求。

        这是「拓扑由 Agent 自主决定」的落点 —— 建链不是免费的：
        每条链路都占掉一份预算，所以要有明确的接受标准。
        默认策略：
          * 问的是我负责的领域 → 接受（这是我最该服务的人）；
          * 对方在回答我、或向我汇报 → 接受（礼貌且必要）；
          * 其余情况一律拒绝，避免拓扑无意义地稠密化。
        """
        if msg.topic in self.owns:
            return True
        if msg.kind in (ANSWER, FINAL) or msg.origin_kind in (ANSWER, FINAL):
            return True
        return self.open_link

    # ------------------------------------------------------------------
    def think(self, agent: Agent, batch: list[Message], board: Blackboard) -> list[Message]:
        out: list[Message] = []

        # 0) 枢纽职责：代投被拓扑挡住的消息
        for m in batch:
            if m.relay_to:
                content = m.content
                if self.summarize_chars and len(content) > self.summarize_chars:
                    content = content[: self.summarize_chars] + "…（枢纽摘要，原文已截断）"
                out.append(
                    m.clone(
                        recipients=(m.relay_to,),
                        relay_to=None,
                        relayed_by=agent.id,
                        content=content,
                    )
                )
                continue

            # R1：答所问
            reply = self._maybe_answer(agent, m)
            if reply:
                out.append(reply)

            # 记录我新学到的事实（来自别人的 ANSWER / FINDING）
            if m.kind in (ANSWER, FINDING) and m.topic not in agent.notes:
                agent.notes[m.topic] = m.content
            if m.kind == FINAL:
                agent.notes[m.topic] = m.content

        # R2：顺藤摸瓜 —— 已知 A 则主动追问 B
        for known_topic, need_topic in self.triggers:
            if known_topic not in agent.notes or need_topic in agent.notes:
                continue
            if need_topic in self._asked:
                continue
            self._asked.add(need_topic)
            owner = board.owner(need_topic)
            out.append(
                Message(
                    sender=agent.id,
                    recipients=(owner,) if owner else ("*",),
                    kind=QUERY,
                    topic=need_topic,
                    content=f"我需要 {need_topic} 的信息，请协助核实。",
                )
            )

        # R3：达成结论
        if self.hypothesis:
            need, out_topic, template = self.hypothesis
            if need <= set(agent.notes) and out_topic not in agent.notes:
                conclusion = template.format(**agent.notes)
                agent.notes[out_topic] = conclusion
                board.publish(out_topic, conclusion, agent.id)
                agent.done = True
                # 结论一律沉淀到共享黑板；report_to 为空表示「只登记，不外发」
                if self.report_to:
                    out.append(
                        Message(
                            sender=agent.id,
                            recipients=(self.report_to,),
                            kind=FINAL,
                            topic=out_topic,
                            content=conclusion,
                        )
                    )

        return out

    # ------------------------------------------------------------------
    def _maybe_answer(self, agent: Agent, m: Message) -> Message | None:
        if m.kind not in (TASK, QUERY):
            return None
        topic = m.topic
        if topic not in self.owns or topic in self._answered:
            return None
        self._answered.add(topic)
        content = self.facts.get(topic, "（无可用信息）")
        agent.notes[topic] = content
        return Message(
            sender=agent.id,
            recipients=(m.sender,),
            kind=ANSWER,
            topic=topic,
            content=content,
            reply_to=m.msg_id,
        )


class EchoBrain:
    """最小可用的占位大脑：只应答、不主动追问、不下结论。

    用来演示框架本身，或作为接 LLM 前的骨架。
    """

    def __init__(self, facts: dict[str, str] | None = None, emits: str | None = None) -> None:
        self.facts = dict(facts or {})
        self.emits = emits

    def think(self, agent: Agent, batch: list[Message], board: Blackboard) -> list[Message]:
        out: list[Message] = []
        for m in batch:
            if m.relay_to:
                out.append(m.clone(recipients=(m.relay_to,), relay_to=None, relayed_by=agent.id))
                continue
            if m.kind in (TASK, QUERY):
                content = self.facts.get(m.topic, "（无信息）")
                if self.emits and m.topic not in agent.notes:
                    content = f"{self.emits}｜{content}"
                out.append(
                    Message(sender=agent.id, recipients=(m.sender,), kind=ANSWER, topic=m.topic, content=content)
                )
        return out


def broadcast(agent: Agent, topic: str, content: str, kind: str = INFORM) -> Message:
    """把一条消息广播给所有其他 Agent —— 对等发现（谁懂谁来答）的最小手段。"""
    return Message(sender=agent.id, recipients=("*",), kind=kind, topic=topic, content=content)
