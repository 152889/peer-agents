"""运行时与基础设施层的回归测试。

这里钉住的是「程序能不能被信任」这一类问题，而不是「结论对不对」：

  * 一次失败必须**看得出来是失败** —— 最危险的不是报错，是安静地返回空；
  * 一个成员崩了不该拖垮整队，在途消息也不能凭空蒸发；
  * 一次实验只能跑一次，复跑必须明确报错而不是给出假的成功；
  * 记忆要有界（去重表），trace 要可复现（确定性编号）。

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M
from peer_agents import scenario_dirty as S
from peer_agents import scenario_incident as I
from peer_agents.agent import Agent, EchoBrain
from peer_agents.blackboard import Blackboard
from peer_agents.bus import MessageBus
from peer_agents.runtime import BUDGET, MAX_ROUNDS, QUIESCENT, Runtime

HUB = "sre"


def dirty(disp: str = "skeptical", topo: str = "peer") -> Runtime:
    rt = S.build(disposition=disp, topology=topo, hub=HUB)
    S.kickoff(rt)
    return rt


def clean() -> Runtime:
    rt = I.build(topology="peer")
    rt.bus.send(M.Message(sender=HUB, recipients=("gateway",), kind=M.TASK,
                          topic="symptom_scope", content="checkout P99 劣化，请定位。"))
    return rt


class Boom:
    """一个每次被轮到都会炸的大脑，并且坚称自己还有活没干完。"""

    def __init__(self, message: str = "数据库连接池炸了") -> None:
        self.message = message

    def think(self, agent, batch, board):
        raise RuntimeError(self.message)

    def has_pending_work(self, board):
        return True


class Quiet:
    """什么都不做，但也绝不报错 —— 用来对照「沉默的成员」。"""

    def think(self, agent, batch, board):
        return []


# ---------------------------------------------------------------------------


class TestOutcomeVisibility(unittest.TestCase):
    """失败必须在结果里可见 —— 不能只靠 final_report == "" 去猜。"""

    def test_successful_run_is_reported_as_completed(self):
        r = dirty().run("ok", ground_truth_tokens=S.TOKENS)
        self.assertTrue(r.completed)
        self.assertTrue(r.ok)
        self.assertEqual(r.exit_reason, QUIESCENT)
        self.assertEqual(r.unresolved, {})
        self.assertTrue(r.describe_outcome().startswith("[✓] 已结案"))

    def test_quiet_failure_is_not_disguised_as_success(self):
        """最危险的一种失败：没有异常、没有报错，只是安静地没产出。"""
        rt = S.build(disposition="skeptical", topology="peer", hub=HUB)
        S.kickoff(rt)
        r = rt.run("starved", ground_truth_tokens=S.TOKENS)
        self.assertTrue(r.completed)          # 这个配置本身是能跑通的
        self.assertTrue(r.ok)

        # 换成「知情者失忆」：需求永远凑不齐
        rt2 = S.build(disposition="skeptical", topology="peer", hub=HUB)
        for a in rt2.agents:
            if a.id == "ordersvc":
                a.brain.knowledge = {}
                a.brain.unresponsive_times = 0
        S.kickoff(rt2)
        r2 = rt2.run("starved")
        self.assertFalse(r2.completed, "没产出终态交付物就不能算完成")
        self.assertFalse(r2.ok)
        self.assertEqual(r2.final_report, "")
        self.assertTrue(r2.unresolved, "必须说清楚是谁缺了什么")
        self.assertIn("未结案", r2.describe_outcome())
        self.assertIn("query_change", r2.describe_outcome())

    def test_exit_reason_distinguishes_budget_from_round_cap(self):
        rt = dirty()
        rt.max_messages = 8
        r = rt.run("budget")
        self.assertEqual(r.exit_reason, BUDGET)
        self.assertTrue(r.hit_budget)

        rt2 = dirty()
        rt2.max_rounds = 2
        r2 = rt2.run("cap")
        self.assertEqual(r2.exit_reason, MAX_ROUNDS)
        self.assertFalse(r2.hit_budget)
        # 两者都没结案，但原因不同 —— 这正是 exit_reason 存在的意义
        self.assertNotEqual(r.describe_outcome().split("——")[1],
                            r2.describe_outcome().split("——")[1])

    def test_unresolved_spells_out_why_each_topic_is_missing(self):
        rt = dirty()
        rt.max_rounds = 2
        r = rt.run("cap")
        joined = " ".join(v for items in r.unresolved.values() for v in items)
        self.assertIn("尚未询问", joined, "「还没轮到问」和「问了不回」必须能区分开")


class TestFaultIsolation(unittest.TestCase):
    """一个成员崩了，不该拖垮整支队伍。"""

    def _rig(self, brain, max_faults: int = 3, rounds: int = 12):
        bus = MessageBus()
        board = Blackboard()
        agents = [
            Agent("bomb", "Boom", brain, bus),
            Agent("ok", "OK", EchoBrain(facts={"t": "我很好"}), bus),
        ]
        rt = Runtime(agents, bus, board, max_rounds=rounds, max_faults_per_agent=max_faults)
        bus.send(M.Message(sender="ok", recipients=("bomb",), kind=M.QUERY,
                           topic="t", content="在吗"))
        return rt, agents

    def test_crash_does_not_abort_the_whole_run(self):
        rt, _agents = self._rig(Boom())
        r = rt.run("crash")          # 不抛异常 = 已隔离
        self.assertEqual(len(r.faults), 3, "连续故障到阈值才摘除")
        self.assertEqual(r.faults[0]["agent"], "bomb")
        self.assertIn("RuntimeError", r.faults[0]["error"])
        self.assertIn("数据库连接池炸了", r.describe_outcome())

    def test_member_is_removed_after_repeated_faults(self):
        rt, agents = self._rig(Boom(), max_faults=2)
        rt.run("crash")
        self.assertFalse(agents[0].alive, "反复出错的成员应被摘除，不再拖住会话")
        self.assertTrue(agents[1].alive, "其他成员不受影响")

    def test_inflight_messages_go_to_dead_letters_not_oblivion(self):
        """在途消息已经从信箱里取出来了 —— 若只是往下走，它就凭空蒸发了。"""
        rt, _agents = self._rig(Boom(), max_faults=1)
        r = rt.run("crash")
        self.assertTrue(r.dead_letters, "应留下死信记录")
        sender, msg = r.dead_letters[0]
        self.assertEqual(sender, "bomb")
        self.assertEqual(msg.content, "在吗")
        self.assertIn("死信", r.describe_outcome())

    def test_a_silent_member_is_not_a_fault(self):
        """「什么都不做」和「崩了」是两回事，不能混为一谈。"""
        rt, agents = self._rig(Quiet(), max_faults=1)
        r = rt.run("quiet")
        self.assertEqual(r.faults, [], "不做事不算故障")
        self.assertTrue(agents[0].alive)


class TestRerunGuard(unittest.TestCase):
    def test_second_run_is_refused_with_actionable_message(self):
        rt = dirty()
        rt.run("first")
        with self.assertRaises(RuntimeError) as ctx:
            rt.run("second")
        self.assertIn("build()", str(ctx.exception), "报错要告诉人怎么补救")

    def test_a_fresh_instance_runs_fine(self):
        dirty().run("a")
        dirty().run("b")     # 每次都重新 build，互不影响


class TestDeterminism(unittest.TestCase):
    """两个「同配置」的运行必须能逐字节 diff —— 这才叫可复现。"""

    @staticmethod
    def _snapshot(rt: Runtime):
        r = rt.run("x")
        return [
            (m.msg_id, m.sender, m.recipients, m.kind, m.topic, m.content,
             m.attempt, m.hop, m.relay_to, m.relayed_by)
            for m in r.messages
        ]

    def test_two_dirty_runs_produce_identical_traces(self):
        self.assertEqual(self._snapshot(dirty()), self._snapshot(dirty()))

    def test_two_clean_runs_produce_identical_traces(self):
        self.assertEqual(self._snapshot(clean()), self._snapshot(clean()))

    def test_message_ids_start_from_a_known_point(self):
        rt = dirty()
        r = rt.run("x")
        self.assertEqual(r.messages[0].msg_id, "m00000",
                         "编号应从零开始，否则跨运行对不上")


class TestBoundedDedup(unittest.TestCase):
    def _bus(self, window: int) -> MessageBus:
        bus = MessageBus(policy=lambda *_: True, dedup_window=window)
        bus.register("a", "b")
        return bus

    def test_seen_table_does_not_grow_without_bound(self):
        bus = self._bus(window=100)
        for i in range(5000):
            bus.send(M.Message(sender="a", recipients=("b",), kind=M.QUERY,
                               topic=f"t{i}", content=str(i)))
        self.assertEqual(len(bus._seen), 100, "去重表必须是有界的")

    def test_dedup_still_works_inside_the_window(self):
        bus = self._bus(window=10)
        m = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="同一条")
        self.assertEqual(bus.send(m), 1)
        self.assertEqual(bus.send(m.clone()), 0, "窗口内仍要拦重复")
        self.assertIn("duplicate", {why for _m, _d, why in bus.dropped})

    def test_old_signatures_are_evicted_so_long_runs_do_not_false_drop(self):
        """窗口之外的老签名会被挤掉 —— 这是刻意的：
        去重拦的是短窗口内的「绕圈」，不是让一个 Agent 永远不能再说同一句话。"""
        bus = self._bus(window=1)
        first = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="C")
        self.assertEqual(bus.send(first), 1)
        for i in range(3):
            bus.send(M.Message(sender="a", recipients=("b",), kind=M.QUERY,
                               topic=f"filler{i}", content="f"))
        again = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="C")
        self.assertEqual(bus.send(again), 1, "老签名被挤出窗口后不应再被误判为重复")


class TestScenarioApiConsistency(unittest.TestCase):
    """两个场景的入口必须长得一样 —— 不一致的 API 本身就是不成熟。"""

    def test_both_builds_return_a_plain_runtime(self):
        self.assertIsInstance(I.build(), Runtime)
        self.assertIsInstance(S.build(), Runtime)

    def test_board_is_reachable_from_the_runtime(self):
        rt = I.build()
        self.assertIsInstance(rt.board, Blackboard)


if __name__ == "__main__":
    unittest.main(verbosity=2)
