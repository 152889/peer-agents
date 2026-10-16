"""脏环境（scenario_dirty）的回归测试。

这里钉住的每一条，都对应开发过程中真实踩过的一个坑：

  * 高置信度的旧信息必须被拦下 —— 只查置信度是不够的；
  * 「问了没人答」的重试不能被两件事吃掉：去重逻辑，和静默检测；
  * 枢纽角色必须真的转发，而不是把过路消息悄悄吞掉；
  * 保真度由认知纪律决定，与拓扑无关 —— 这是本实验的核心结论。

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M
from peer_agents import scenario_dirty as S
from peer_agents.blackboard import Blackboard
from peer_agents.brain_dirty import GULLIBLE, SKEPTICAL, DirtyBrain
from peer_agents.bus import MessageBus
from peer_agents.facts import Fact, best_of

HUB = "sre"


def run(disp: str, topo: str):
    rt = S.build(disposition=disp, topology=topo, hub=HUB)
    S.kickoff(rt)
    res = rt.run(f"{disp}/{topo}", ground_truth_tokens=S.TOKENS)
    return rt, res


def brain_of(rt, aid: str) -> DirtyBrain:
    return next(a.brain for a in rt.agents if a.id == aid)


# ---------------------------------------------------------------------------


class TestFact(unittest.TestCase):
    def test_encode_decode_roundtrip(self):
        f = Fact("索引存在", confidence=0.92, as_of=-100, evidence="快照", source="dba")
        back = Fact.decode(f.encode())
        self.assertEqual(back, f)

    def test_live_fact_has_null_timestamp_and_never_ages(self):
        f = Fact("实时情况", confidence=0.9)
        self.assertIsNone(f.as_of)
        self.assertEqual(f.age(999), 0)
        self.assertFalse(f.is_stale(999, fresh_within=1))
        self.assertIn('"t": null', f.encode(), "序列化后应保持「无时间戳」而不是伪造一个 0")

    def test_stamp_freezes_live_fact_but_not_snapshot(self):
        live = Fact("实时").stamp(7)
        self.assertEqual(live.as_of, 7, "实时信息应在出网那一刻落成绝对刻度")
        snap = Fact("快照", as_of=-100).stamp(7)
        self.assertEqual(snap.as_of, -100, "历史快照自带刻度，不应被盖章覆盖")

    def test_unparsable_payload_is_not_trusted(self):
        self.assertIsNone(Fact.decode("这不是 JSON"))
        self.assertIsNone(Fact.decode(""))
        self.assertIsNone(Fact.decode('{"no_value": 1}'))

    def test_trust_requires_both_confidence_and_freshness(self):
        now = 10
        self.assertTrue(Fact("ok", 0.90, now).is_trusted(now, 0.75, 25))
        self.assertFalse(Fact("低置信", 0.60, now).is_trusted(now, 0.75, 25))
        # 这条最要命：置信度很高，但它是 100 个时间单位前的快照
        self.assertFalse(Fact("旧但权威", 0.99, -100).is_trusted(now, 0.75, 25))

    def test_fresh_beats_authoritative_when_stale(self):
        """过期的权威记录，不如新鲜的普通记录 —— 这是最容易翻车的地方。"""
        stale_authority = Fact("旧的权威结论", confidence=0.99, as_of=-100)
        fresh_mediocre = Fact("新的普通结论", confidence=0.80, as_of=0)
        self.assertEqual(best_of([stale_authority, fresh_mediocre]), fresh_mediocre)
        # 同一时间刻度上，才轮到比置信度
        self.assertEqual(
            best_of([Fact("a", 0.80, 0), Fact("b", 0.95, 0)]).value, "b",
        )


class TestBusRetry(unittest.TestCase):
    def test_retry_is_not_swallowed_by_dedup(self):
        """重试报文正文与首次一字不差，但它是**新的一次发送**，不是同一条消息绕回来。"""
        bus = MessageBus()
        bus.register("a", "b")
        first = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t",
                          content="请提供 t", attempt=0)
        again = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t",
                          content="请提供 t", attempt=1)
        self.assertEqual(bus.send(first), 1)
        self.assertEqual(bus.send(again), 1, "重试不应被当作重复消息丢弃")
        self.assertEqual(len(bus.boxes["b"]), 2)

    def test_true_duplicate_is_still_dropped(self):
        """放宽重试不能把去重本身弄坏 —— 同一次尝试重复投递仍要拦下。"""
        bus = MessageBus()
        bus.register("a", "b")
        m = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t",
                      content="同一条", attempt=0)
        self.assertEqual(bus.send(m), 1)
        dup = m.clone()
        self.assertEqual(bus.send(dup), 0)
        self.assertIn("duplicate", {why for _m, _d, why in bus.dropped})


class TestDirtyBrain(unittest.TestCase):
    def _agent(self, brain: DirtyBrain, aid: str = "me"):
        from peer_agents.agent import Agent
        bus = MessageBus()
        return Agent(aid, "test", brain, bus)

    def test_gullible_accepts_first_arrival_whatever_it_is(self):
        b = DirtyBrain(disposition=GULLIBLE)
        a = self._agent(b)
        board = Blackboard()
        board.round = 5
        b.think(a, [M.Message(sender="x", recipients=("me",), kind=M.ANSWER, topic="t",
                              content=Fact("先到的（其实很旧）", 0.99, -999).encode())],
                board)
        self.assertIn("t", b.ledger, "轻信策略不问新旧，先到先得")
        self.assertEqual(b.stale_accepts(5), 1, "它当场收下了一条过期信息")

    def test_skeptical_rejects_high_confidence_stale_snapshot(self):
        b = DirtyBrain(disposition=SKEPTICAL, trust_threshold=0.75, fresh_within=25)
        a = self._agent(b)
        board = Blackboard()
        board.round = 10
        b.think(a, [M.Message(sender="x", recipients=("me",), kind=M.ANSWER, topic="t",
                              content=Fact("索引存在", 0.92, -100).encode())], board)
        self.assertNotIn("t", b.ledger, "高置信度但过期，仍然不可采信")
        self.assertEqual(b.stale_accepts(10), 0)

    def test_skeptical_self_corrects_when_better_evidence_arrives(self):
        b = DirtyBrain(disposition=SKEPTICAL, trust_threshold=0.75, fresh_within=25)
        a = self._agent(b)
        board = Blackboard()
        board.round = 3
        b.think(a, [M.Message(sender="deploy", recipients=("me",), kind=M.ANSWER, topic="t",
                              content=Fact("无 DDL", 0.85, 3).encode())], board)
        self.assertEqual(b.ledger["t"].value, "无 DDL")
        board.round = 4
        b.think(a, [M.Message(sender="audit", recipients=("me",), kind=M.ANSWER, topic="t",
                              content=Fact("有一条已回滚的 DDL", 0.95, 4).encode())], board)
        self.assertEqual(b.ledger["t"].value, "有一条已回滚的 DDL")
        self.assertEqual(b.overrides, 1, "更高置信度的版本到达时应覆盖先前结论")
        self.assertIn("t", b.conflicts, "两个版本不一致，应记录为分歧")

    def test_pending_work_is_reported_even_while_waiting_to_retry(self):
        """这是「死区」那个 bug 的回归。

        判断的是「有没有未了结的提问」，**不是**「现在是不是重试时机」。
        若只在时机到了才返回 True，等待窗口内 Agent 不会被唤醒、
        系统看起来已经静默，运行时会提前收敛 —— 重试永远等不到窗口打开。
        """
        b = DirtyBrain(disposition=SKEPTICAL, max_attempts=3, ask_budget=5)
        board = Blackboard()
        board.round = 4
        b.ask_log[("t", "peer")] = [4]      # 第 4 轮问过，刚过 0 轮
        self.assertTrue(b.has_pending_work(board), "还欠着一个答复，就不该被判定为无事可做")

    def test_exhausted_attempts_release_the_system(self):
        b = DirtyBrain(disposition=SKEPTICAL, max_attempts=3, ask_budget=5)
        board = Blackboard()
        board.round = 99
        b.ask_log[("t", "peer")] = [1, 2, 3]   # 已用完三次
        self.assertFalse(b.has_pending_work(board), "试满之后必须放手，否则永不收敛")

    def test_exhausted_budget_releases_the_system(self):
        b = DirtyBrain(disposition=SKEPTICAL, max_attempts=3, ask_budget=2)
        board = Blackboard()
        board.round = 99
        b.budget_used = 2
        b.ask_log[("t", "peer")] = [1]
        self.assertFalse(b.has_pending_work(board), "想重试但没预算，不应拖住整个系统")

    def test_attempt_numbers_are_contiguous_from_zero(self):
        """首次提问的尝试编号必须是 0，否则收件人按编号识别重复时会被绕过去。"""
        board = Blackboard()
        board.round = 1
        board.publish_directory({"t": ["peer"]})
        b = DirtyBrain(disposition=GULLIBLE, ask_budget=5)
        a = self._agent(b, "me")
        out = []
        for r in (1, 3, 5):
            board.round = r
            b._ask(a, "t", out, board, r, reason="test")
        self.assertEqual([m.attempt for m in out], [0, 1, 2])

    def test_hub_forwards_relayed_message_instead_of_eating_it(self):
        """枢纽把被拓扑挡住的消息代投出去。

        少了这一步，星型拓扑下跨部门的每条消息都会在枢纽处静默消失，
        外部只表现为「下游拿不到信息」——极难排查。
        """
        b = DirtyBrain(disposition=SKEPTICAL)
        a = self._agent(b, HUB)
        relayed = M.Message(sender="dba", recipients=(HUB,), kind=M.QUERY, topic="t",
                            content="请提供 t", relay_to="deploy")
        out = b.think(a, [relayed], Blackboard())
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].recipients, ("deploy",))
        self.assertIsNone(out[0].relay_to)
        self.assertEqual(out[0].relayed_by, HUB, "代投后应打上已投递标记，避免绕圈")


class TestDirtyScenario(unittest.TestCase):
    def test_skeptical_reaches_full_fidelity_under_both_topologies(self):
        for topo in ("peer", "star"):
            with self.subTest(topology=topo):
                _rt, r = run(SKEPTICAL, topo)
                self.assertEqual(r.fidelity, 1.0, "交叉验证应还原全部关键信息")
                self.assertIn("排除", r.final_report, "干扰项必须被明确排除")

    def test_gullible_is_fooled_under_both_topologies(self):
        for topo in ("peer", "star"):
            with self.subTest(topology=topo):
                _rt, r = run(GULLIBLE, topo)
                self.assertLess(r.fidelity, 1.0, "轻信策略应被过期快照或传闻带偏")

    def test_fidelity_is_decided_by_discipline_not_topology(self):
        """本实验的核心结论：换成对等拓扑，救不了不设防的认知。"""
        _rt, sk_p = run(SKEPTICAL, "peer")
        _rt, sk_s = run(SKEPTICAL, "star")
        _rt, gu_p = run(GULLIBLE, "peer")
        _rt, gu_s = run(GULLIBLE, "star")
        self.assertEqual(sk_p.fidelity, sk_s.fidelity, "认知风格相同时，拓扑不应改变结论质量")
        self.assertEqual(gu_p.fidelity, gu_s.fidelity)
        self.assertGreater(sk_p.fidelity, gu_p.fidelity)

    def test_topology_only_changes_the_price(self):
        _rt, sk_p = run(SKEPTICAL, "peer")
        _rt, sk_s = run(SKEPTICAL, "star")
        self.assertLess(sk_p.data_messages, sk_s.data_messages, "对等应更省")
        self.assertLess(sk_p.hub_load(HUB), sk_s.hub_load(HUB), "星型把开销全压给枢纽")
        self.assertGreater(sk_p.lateral(HUB), 0, "对等下有消息绕开枢纽")
        self.assertEqual(sk_s.lateral(HUB), 0, "星型下子 Agent 之间没有直连")

    def test_gullible_only_asks_the_first_knower(self):
        rt, _r = run(GULLIBLE, "peer")
        self.assertEqual(len(brain_of(rt, "audit").ledger), 0,
                         "轻信策略下，排在后面的知情者从头到尾没被问过")

    def test_unresponsive_member_is_retried_until_it_answers(self):
        rt, _r = run(SKEPTICAL, "peer")
        sre = brain_of(rt, "sre")
        self.assertIn("query_change", sre.ledger, "重试最终应拿到订单服务的答复")
        self.assertGreaterEqual(len(sre.ask_log[("query_change", "ordersvc")]), 2,
                                "前两次不回应，必须触发至少一次重试")
        self.assertEqual(brain_of(rt, "ordersvc").silent_count, 2,
                         "订单服务应当只沉默约定的那两次")

    def test_stale_snapshot_and_rumor_are_both_rejected(self):
        rt, _r = run(SKEPTICAL, "peer")
        sre = brain_of(rt, "sre")
        stale = [f for f in sre.rejected.get("index_state", []) if f.source == "dba"]
        rumor = [f for f in sre.rejected.get("rate_limit", []) if f.source == "gateway"]
        self.assertTrue(stale and stale[0].as_of == S.STALE, "应拒收 DBA 的过期快照")
        self.assertTrue(rumor and rumor[0].confidence < 0.75, "应拒收网关的低置信度传闻")

    def test_run_converges_well_before_the_round_cap(self):
        for disp in (SKEPTICAL, GULLIBLE):
            for topo in ("peer", "star"):
                with self.subTest(disp=disp, topo=topo):
                    _rt, r = run(disp, topo)
                    self.assertLess(r.rounds, 30, "不应靠耗尽轮次上限才停下来")
                    self.assertNotEqual(r.final_report, "", "必须真的产出终报")


if __name__ == "__main__":
    unittest.main(verbosity=2)
