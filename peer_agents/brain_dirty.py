"""在「脏」环境里工作的大脑。

与 RuleBrain 的区别,不在于会不会推理,而在于**怎么对待信息本身**。
RuleBrain 的所有输入都是干净的一手事实,「问谁」是唯一要决定的事。
真实环境里多了一层:「这条信息我该不该信?」

两种认知风格
-----------
``gullible``  轻信:每个 topic 只问**第一个**知情者,收到什么信什么。
              成本最低、速度最快,但它把「先被问到」当成了「更可信」。

``skeptical`` 怀疑:问遍所有知情者,比对置信度与时间戳,
              并且**允许自我更正**（更高置信度的版本到达时,覆盖先前的结论）。

这个对比是本实验的杠杆。它要回答的是:
**在脏环境里决定成败的,是通信拓扑,还是每个 Agent 的信息纪律?**

提问预算
-------
提问不是免费的。``ask_budget`` 强制 Agent 在「问清楚」和「问得起」之间取舍。
没有这个约束,「全部问一遍」永远是最优解,实验就失去意义了。
"""

from __future__ import annotations

from .agent import Agent
from .blackboard import Blackboard
from .facts import Fact, best_of
from .message import ANSWER, FINAL, FINDING, QUERY, TASK, Message

GULLIBLE = "gullible"
SKEPTICAL = "skeptical"


class DirtyBrain:
    def __init__(
        self,
        knowledge: dict[str, Fact] | None = None,
        disposition: str = SKEPTICAL,
        trust_threshold: float = 0.75,
        fresh_within: int = 3,
        ask_budget: int = 15,
        triggers: tuple[tuple[str, str], ...] = (),
        requirement: tuple[set[str], str, str] | None = None,
        notify: str | None = None,
        report_to: str | None = None,
        unresponsive_times: int = 0,
        max_attempts: int = 3,
    ) -> None:
        self.knowledge = dict(knowledge or {})
        self.disposition = disposition
        self.trust_threshold = trust_threshold
        self.fresh_within = fresh_within
        self.ask_budget = ask_budget
        self.triggers = list(triggers)
        self.requirement = requirement
        self.notify = notify
        self.report_to = report_to
        self.unresponsive_times = unresponsive_times
        self.max_attempts = max_attempts

        # -- 认知状态（可观测） -------------------------------------------
        self.ledger: dict[str, Fact] = {}                 # 最终采信的版本
        self.candidates: dict[str, list[Fact]] = {}       # 见过的所有版本
        self.conflicts: dict[str, list[Fact]] = {}        # 出现分歧的 topic
        self.rejected: dict[str, list[Fact]] = {}         # 被我判定不可信的
        self.trace: list[str] = []                        # 行为轨迹
        self.ask_log: dict[tuple[str, str], list[int]] = {}
        self.budget_used = 0
        self.silent_count = 0
        self.overrides = 0          # 自我更正次数
        self.exhausted: set[tuple[str, str]] = set()
        self._notified: dict[str, str] = {}

    # ==================================================================
    def think(self, agent: Agent, batch: list[Message], board: Blackboard) -> list[Message]:
        out: list[Message] = []
        now = board.round
        for m in batch:
            # 枢纽职责：代投被拓扑挡住的消息。
            # 这是**纯转发** —— 不改内容、不掺解释，只把自己当中继。
            # 少了这一段，星型拓扑下跨部门的每一条消息都会在枢纽这里消失，
            # 而这种「静默吞掉」在外部看只表现为「下游拿不到信息」。
            if m.relay_to:
                out.append(m.clone(recipients=(m.relay_to,), relay_to=None,
                                   relayed_by=agent.id))
                continue
            if m.kind in (TASK, QUERY):
                self._on_query(agent, m, out, now)
            elif m.kind in (ANSWER, FINDING):
                self._on_answer(agent, m, out, board, now)
        self._drive(agent, out, board, now)
        self._conclude(agent, out, now)
        return out

    # -- 被问到 ---------------------------------------------------------
    def _on_query(self, agent: Agent, m: Message, out: list[Message], now: int) -> None:
        if self.unresponsive_times and self.silent_count < self.unresponsive_times:
            self.silent_count += 1
            self.trace.append(f"{m.sender} 问我「{m.topic}」→ 我正在处理线上问题，未回应")
            return
        fact = self.knowledge.get(m.topic)
        if fact is None:
            self.trace.append(f"{m.sender} 问我「{m.topic}」→ 我不掌握这个信息")
            self.exhausted.add((m.topic, agent.id))
            out.append(Message(sender=agent.id, recipients=(m.sender,), kind=ANSWER,
                               topic=m.topic, content="", reply_to=m.msg_id))
            return
        self.trace.append(f"回答 {m.sender}：{m.topic} {fact.brief()}")
        # 出网那一刻给「实时」信息盖章（快照类信息自带历史刻度，不受影响）
        sent = fact.with_source(agent.id).stamp(now)
        # 我答出来的东西，我自己当然也知道 —— 否则「被问过」就不会引发后续追问
        self.ledger.setdefault(m.topic, sent)
        out.append(Message(sender=agent.id, recipients=(m.sender,), kind=ANSWER,
                           topic=m.topic, content=sent.encode(),
                           reply_to=m.msg_id))

    # -- 收到信息 -------------------------------------------------------
    def _on_answer(self, agent: Agent, m: Message, out: list[Message],
                   board: Blackboard, now: int) -> None:
        fact = Fact.decode(m.content)
        if fact is None:
            if m.content == "":
                self.trace.append(f"{m.sender} 表示不掌握「{m.topic}」")
            else:
                self.trace.append(f"来自 {m.sender} 的「{m.topic}」无法解析，按无效处理")
            return
        self.candidates.setdefault(m.topic, []).append(fact)
        self._evaluate(agent, m.topic, out, board, now)

    def _evaluate(self, agent: Agent, topic: str, out: list[Message],
                  board: Blackboard, now: int) -> None:
        cands = self.candidates[topic]
        if len({f.value for f in cands}) > 1:
            self.conflicts[topic] = list(cands)

        if self.disposition == GULLIBLE:
            # 轻信：先到的就是对的。这是它最大也最贵的简化。
            if topic not in self.ledger:
                self._accept(agent, topic, cands[0], out, board, now,
                             reason="轻信策略：直接采用最先到达的版本")
            return

        trusted = [f for f in cands if f.is_trusted(now, self.trust_threshold, self.fresh_within)]
        for f in cands:
            if f not in trusted and f not in self.rejected.get(topic, []):
                self.rejected.setdefault(topic, []).append(f)
                reasons = []
                if f.confidence < self.trust_threshold:
                    reasons.append("置信度不足")
                if f.is_stale(now, self.fresh_within):
                    reasons.append(f"信息已过期（{f.age(now)} 个时间单位前）")
                self.trace.append(
                    f"拒绝 {f.source or '?'} 的「{topic}」：{f.brief()}"
                    f" → {'；'.join(reasons) or '不可信'}"
                )
        if not trusted:
            return
        chosen = best_of(trusted)
        if chosen is None:
            return
        current = self.ledger.get(topic)
        if current is None or current.value != chosen.value:
            self._accept(agent, topic, chosen, out, board, now,
                         reason="怀疑策略：取「最新 + 置信度最高」的版本")

    def _accept(self, agent: Agent, topic: str, fact: Fact, out: list[Message],
                board: Blackboard, now: int, reason: str) -> None:
        prev = self.ledger.get(topic)
        self.ledger[topic] = fact
        self.trace.append(f"采信「{topic}」← {fact.source or '?'} {fact.brief()}（{reason}）")
        if prev is not None and prev.value != fact.value:
            self.overrides += 1
            self.trace.append(f"  ↑ 自我更正：覆盖先前的版本 {prev.brief()}")

        # 进展同步：把结论告诉队友，别人就不必再问一遍
        if self.notify and self._notified.get(topic) != fact.value:
            self._notified[topic] = fact.value
            out.append(Message(sender=agent.id, recipients=(self.notify,), kind=FINDING,
                               topic=topic, content=fact.encode()))

    # -- 主动提问 -------------------------------------------------------
    def _drive(self, agent: Agent, out: list[Message], board: Blackboard, now: int) -> None:
        # A. 超时重试：问了没人答，换个时间再问
        for (topic, owner), rounds in list(self.ask_log.items()):
            if topic in self.ledger or (topic, owner) in self.exhausted:
                continue
            self._ask(agent, topic, out, board, now,
                      reason=f"第 {len(rounds) + 1} 次尝试（前次无回应）", only=owner)

        # B. 触发链：知道了 A，就说明还需要 B
        for known_t, need_t in self.triggers:
            if known_t in self.ledger and need_t not in self.ledger:
                self._ask(agent, need_t, out, board, now,
                          reason=f"得知「{known_t}」后，还需要「{need_t}」")

    def _ask(self, agent: Agent, topic: str, out: list[Message], board: Blackboard,
             now: int, reason: str, only: str | None = None) -> None:
        owners = [o for o in board.owners(topic) if o != agent.id]
        if only is not None:
            owners = [o for o in owners if o == only]
        if not owners:
            return
        # 认知风格的唯一分歧点：问一个人，还是问遍所有知情者
        targets = owners if self.disposition == SKEPTICAL else owners[:1]

        for owner in targets:
            key = (topic, owner)
            rounds = self.ask_log.get(key, [])
            if len(rounds) >= self.max_attempts or key in self.exhausted:
                continue
            if rounds and now - rounds[-1] < 2:      # 给对方一点时间（网络与处理时延）
                continue
            if self.budget_used >= self.ask_budget:
                self.trace.append(f"提问预算耗尽，放弃向 {owner} 询问「{topic}」")
                return
            # 注意：计数必须取自**存进字典的那个列表**。
            # 上面 `get(key, [])` 在键不存在时给的是个游离的空列表，
            # 拿它的长度当尝试编号会得到 0、2、3 这种不连续的序列 ——
            # 收件人若按编号识别重复，就会被绕过去。
            log = self.ask_log.setdefault(key, [])
            attempt_no = len(log)
            log.append(now)
            self.budget_used += 1
            who = "全部知情者" if len(targets) > 1 else owner
            self.trace.append(f"向 {who} 询问「{topic}」（{reason}）")
            out.append(Message(sender=agent.id, recipients=(owner,), kind=QUERY, topic=topic,
                               attempt=attempt_no,   # 重试是新的一次发送，不能被去重吞掉
                               content=f"请提供 {topic} 的信息。"))

    # -- 结案 -----------------------------------------------------------
    def _conclude(self, agent: Agent, out: list[Message], now: int) -> None:
        if not self.requirement:
            return
        need, out_topic, template = self.requirement
        if out_topic in self.ledger or not need <= set(self.ledger):
            return
        conclusion = template.format(**{t: self.ledger[t].value for t in need})
        self.ledger[out_topic] = Fact(conclusion, 1.0, now, "综合各方信息")
        self.trace.append(f"结案：{out_topic}")
        out.append(Message(
            sender=agent.id,
            recipients=(self.report_to,) if self.report_to else ("*",),
            kind=FINAL, topic=out_topic, content=conclusion,
        ))

    # -- 供观测使用 -----------------------------------------------------
    def has_pending_work(self, board: Blackboard) -> bool:
        """还有没有「该做但没做完」的事（主要是超时重试）。

        Runtime 靠这个判断某轮要不要唤醒一个信箱空了的 Agent ——
        否则「问了没人答」的消息会随着静默检测一起被埋掉。

        这里有个**必须小心**的点：判断的是「有没有未了结的提问」，
        **不**判断「现在是不是重试的时机」。如果只在时机到了才返回 True，
        那么在等待窗口里 Agent 不会被唤醒、系统看起来已经静默，
        运行时会提前收敛 —— 重试窗口还来不及打开，就被自己的静默检测埋掉了。
        「还欠着一件事」本身，就是「不该收敛」的理由。
        """
        for (topic, owner), rounds in self.ask_log.items():
            if topic in self.ledger or (topic, owner) in self.exhausted:
                continue
            if len(rounds) >= self.max_attempts:
                continue
            # 想问但预算已经用完 → 重试也无从谈起，不再拖住系统
            if self.budget_used >= self.ask_budget:
                return False
            return True
        return False

    def stale_accepts(self, now: int) -> int:
        """被我采信、但已经过期的信息条数 —— 「被骗」的量化指标。"""
        return sum(1 for f in self.ledger.values() if f.is_stale(now, self.fresh_within))

    def low_confidence_accepts(self) -> int:
        return sum(1 for f in self.ledger.values() if f.confidence < self.trust_threshold)
