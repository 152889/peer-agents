"""回归测试：把 README 里每一条结论都钉死成可执行的断言。

只用标准库，不需要装任何依赖：

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M  # noqa: E402
from peer_agents.bus import MessageBus, star_policy  # noqa: E402
from peer_agents.scenario_incident import (  # noqa: E402
    GROUND_TRUTH_TOKENS,
    T_SCOPE,
    build,
)
from peer_agents.topology import Topology  # noqa: E402

HUB = "sre"


def bootstrap(runtime) -> None:
    runtime.bus.send(
        M.Message(
            sender=HUB,
            recipients=("gateway",),
            kind=M.TASK,
            topic=T_SCOPE,
            content="checkout 接口 P99 由 120ms 劣化到 900ms，请定位你这一侧是否有异常。",
        )
    )


def run_scenario(**cfg):
    runtime, _board = build(**cfg)
    bootstrap(runtime)
    return runtime.run(name=cfg.get("topology", "?"), ground_truth_tokens=GROUND_TRUTH_TOKENS)


# ---------------------------------------------------------------------------


class TestTopology(unittest.TestCase):
    def test_bfs_returns_shortest_path(self):
        t = Topology(["a", "b", "c", "d"], budget=3, initial_links=[("a", "b"), ("b", "c"), ("c", "d")])
        self.assertEqual(t.route("a", "d"), ["a", "b", "c", "d"])
        self.assertEqual(t.distance("a", "c"), 2)

    def test_link_shortcut_update_route(self):
        t = Topology(["a", "b", "c", "d"], budget=3, initial_links=[("a", "b"), ("b", "c"), ("c", "d")])
        t.try_link("a", "d")
        self.assertEqual(t.route("a", "d"), ["a", "d"])
        self.assertEqual(t.distance("a", "d"), 1)

    def test_disconnected_node_is_unreachable(self):
        t = Topology(["a", "b", "c"], budget=3, initial_links=[("a", "b")])
        self.assertIsNone(t.route("a", "c"))

    def test_budget_evicts_least_recently_used(self):
        t = Topology(["a", "b", "c", "d"], budget=2, initial_links=[("a", "b")])
        t.try_link("a", "c")          # a: {b, c}，预算已满
        t.touch("a", "c")             # 让 a-c 变「新」，a-b 成为最久未用
        t.try_link("a", "d")
        self.assertFalse(t.has_link("a", "b"), "应淘汰最久未使用的 a-b")
        self.assertTrue(t.has_link("a", "c"))
        self.assertTrue(t.has_link("a", "d"))
        self.assertEqual(t.evictions, 1)

    def test_degree_never_exceeds_budget(self):
        t = Topology(["a", "b", "c", "d", "e"], budget=2, initial_links=[])
        for peer in ("b", "c", "d", "e"):
            t.try_link("a", peer)
        self.assertLessEqual(t.degree()["a"], 2)


class TestBus(unittest.TestCase):
    def _bus(self, *nodes, **kw):
        bus = MessageBus(**kw)
        bus.register(*nodes)
        return bus

    def test_full_mesh_delivers_directly(self):
        bus = self._bus("a", "b")
        bus.send(M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="x"))
        self.assertEqual(len(bus.boxes["b"]), 1)
        self.assertEqual(bus.boxes["b"][0].relay_to, None)

    def test_star_topology_relays_through_hub(self):
        bus = self._bus("a", "b", "h", policy=star_policy("h"), hub="h")
        bus.send(M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="x"))
        self.assertEqual(len(bus.boxes["b"]), 0, "星型下 a 不允许直投 b")
        self.assertEqual(len(bus.boxes["h"]), 1)
        self.assertEqual(bus.boxes["h"][0].relay_to, "b")

    def test_relayed_message_is_not_treated_as_duplicate(self):
        """枢纽代投的报文与原报文 sender/recipients/topic/content 全同，
        若签名不含 relayed_by 就会被误判重复而丢包，中转链直接断掉。"""
        m = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="x")
        self.assertNotEqual(m.signature(), m.clone(relayed_by="h").signature())

    def test_none_hub_does_not_bypass_topology(self):
        """hub 为 None 时 `relayed_by == hub` 会退化成 None == None 恒真，
        必须判空，否则所有消息绕过拓扑直投。"""
        topo = Topology(["a", "b"], budget=1, initial_links=[])
        bus = self._bus("a", "b", topology=topo, accept_hook=lambda *_: True)
        bus.send(M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="x"))
        self.assertEqual(len(bus.boxes["b"]), 0, "无链路时不应直投，而应挂起并协商")

    def test_multihop_routing_records_path(self):
        topo = Topology(["a", "b", "c"], budget=2, initial_links=[("a", "b"), ("b", "c")])
        bus = self._bus("a", "b", "c", topology=topo, accept_hook=lambda *_: False)
        n = bus.send(M.Message(sender="a", recipients=("c",), kind=M.QUERY, topic="t", content="x"))
        self.assertEqual(n, 1)
        self.assertEqual(bus.boxes["c"][0].path, ("a", "b", "c"))
        self.assertEqual(bus.transit_hops, 1)
        self.assertEqual(len(bus.boxes["b"]), 0, "中转节点不应把过路消息留在自己的信箱里")

    def test_unreachable_message_pends_then_delivers_after_negotiation(self):
        topo = Topology(["a", "b"], budget=1, initial_links=[])
        bus = self._bus("a", "b", topology=topo, accept_hook=lambda owner, req, msg: True)
        msg = M.Message(sender="a", recipients=("b",), kind=M.QUERY, topic="t", content="x")
        self.assertEqual(bus.send(msg), 0)
        self.assertEqual(len(bus.pending), 1)
        self.assertTrue(topo.has_link("a", "b"), "应已通过协商建链")
        self.assertEqual(bus.retry_pending(), 1)
        self.assertEqual(len(bus.boxes["b"]), 1)


class TestScenarios(unittest.TestCase):
    def test_every_topology_reaches_the_same_root_cause(self):
        for cfg in (dict(topology="peer"), dict(topology="star"), dict(topology="adaptive")):
            with self.subTest(topology=cfg["topology"]):
                r = run_scenario(**cfg)
                self.assertIn("SeqScan", r.final_report)
                self.assertEqual(r.fidelity, 1.0, "结论不应因拓扑不同而失真")

    def test_hub_summarizing_degrades_fidelity(self):
        r = run_scenario(topology="star", hub_summarize_chars=38)
        self.assertLess(r.fidelity, 1.0, "枢纽转述而非转发时应丢失关键细节")

    def test_peer_is_cheaper_than_star(self):
        p = run_scenario(topology="peer")
        s = run_scenario(topology="star")
        self.assertLess(p.data_messages, s.data_messages)
        self.assertLess(p.rounds, s.rounds)
        self.assertLess(p.hub_load(HUB), s.hub_load(HUB))

    def test_star_relays_but_peer_does_not(self):
        star = run_scenario(topology="star")
        peer = run_scenario(topology="peer")
        self.assertGreater(star.relay_touches, 0, "星型下必有枢纽代投")
        self.assertEqual(star.lateral(HUB), 0, "星型下子 Agent 之间不应有直连")
        self.assertEqual(peer.relay_touches, 0, "对等模式下无需中转")
        self.assertGreater(peer.lateral(HUB), 0, "对等模式下子 Agent 应直接互通")

    def test_adaptive_topology_grows_from_sparse_start(self):
        r = run_scenario(topology="adaptive", link_budget=3)
        self.assertLess(len(r.initial_edges), len(r.final_edges))
        self.assertGreater(r.link_accepts, 0)
        self.assertEqual(r.fidelity, 1.0)

    def test_adaptive_matches_full_mesh_efficiency_without_a_hub(self):
        p = run_scenario(topology="peer")
        a = run_scenario(topology="adaptive", link_budget=3)
        self.assertEqual(a.data_messages, p.data_messages)
        self.assertEqual(a.hub_load(HUB), p.hub_load(HUB))

    def test_tight_budget_forces_eviction_but_keeps_result(self):
        r = run_scenario(topology="adaptive", link_budget=2)
        self.assertGreater(r.evictions, 0, "预算收紧时应发生淘汰")
        self.assertEqual(r.fidelity, 1.0, "淘汰不应影响结论")

    def test_pending_queue_blocks_quiescence(self):
        """收敛判定必须同时看信箱和挂起队列，否则等建链的消息会被静默遗弃。"""
        r = run_scenario(topology="adaptive", link_budget=2)
        self.assertNotIn("unreachable-after-negotiation", {why for _m, _d, why in r.dropped})
        self.assertNotEqual(r.final_report, "", "不应在消息仍在途时判定收敛")


class TestLLMBrain(unittest.TestCase):
    """LLMBrain 的协议层验证。

    这里用本地桩服务作为对端，验证的是「HTTP 协议层是否忠实」：
    请求是否成形、响应是否被正确解析、解析出的消息能否驱动整条链路。
    它**不能**回答「真实模型表现如何」——那是另一个问题。
    """

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
        import mock_llm_server

        cls.mock = mock_llm_server
        cls._saved = {k: os.environ.get(k) for k in ("OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL")}
        cls.server, base_url = mock_llm_server.start()
        os.environ["OPENAI_BASE_URL"] = base_url
        os.environ["OPENAI_API_KEY"] = "mock-key"
        os.environ["OPENAI_MODEL"] = "mock-llm"

    def setUp(self):
        # 桩是有状态的，每个用例开始前必须重置，否则用例之间会串味
        self.mock.reset()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        for k, v in cls._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_llm_path_reaches_same_conclusion_as_rule_path(self):
        r = run_scenario(topology="peer", brain_mode="llm")
        self.assertIn("SeqScan", r.final_report)
        self.assertEqual(r.fidelity, 1.0)

    def test_llm_path_produces_same_report_text(self):
        llm = run_scenario(topology="peer", brain_mode="llm")
        rule = run_scenario(topology="peer", brain_mode="rule")
        self.assertEqual(llm.final_report, rule.final_report, "协议层不应改变结论文本")

    def test_prompt_contains_every_required_section(self):
        runtime, _ = build(topology="peer", brain_mode="llm")
        bootstrap(runtime)
        runtime.run(name="llm", ground_truth_tokens=GROUND_TRUTH_TOKENS)
        prompts = [req["prompt"] for a in runtime.agents
                   for req in getattr(a.brain, "requests", [])]
        self.assertTrue(prompts, "应当真实发出过请求")
        joined = "\n".join(prompts)
        for section in ("# 你的身份", "# 你掌握的私有信息", "# 团队共享黑板", "# 可用 topic", "# 输出格式", "# 本轮收到的消息"):
            self.assertIn(section, joined, f"prompt 缺少段落：{section}")
        self.assertIn("slow_query → 负责人 dba", joined, "prompt 里应带上通讯录")

    def test_request_is_well_formed(self):
        runtime, _ = build(topology="peer", brain_mode="llm")
        bootstrap(runtime)
        runtime.run(name="llm", ground_truth_tokens=GROUND_TRUTH_TOKENS)
        req = next(req for a in runtime.agents for req in getattr(a.brain, "requests", []))
        self.assertTrue(req["url"].endswith("/chat/completions"))
        self.assertEqual(req["payload"]["model"], "mock-llm")
        self.assertEqual(req["payload"]["messages"][0]["role"], "user")
        self.assertTrue(req["payload"]["messages"][0]["content"])

    def test_missing_credentials_raise_clear_error(self):
        from peer_agents.llm import LLMBrain

        brain = LLMBrain(persona="你是「x」", skills={})
        saved = os.environ.pop("OPENAI_API_KEY", None)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                brain._call("prompt")
            self.assertIn("OPENAI_API_KEY", str(ctx.exception))
        finally:
            if saved:
                os.environ["OPENAI_API_KEY"] = saved

    def test_malformed_response_does_not_crash(self):
        """模型不听话、返回一堆废话时，应退化为「本轮不发消息」而不是崩掉。"""
        from peer_agents.llm import LLMBrain

        brain = LLMBrain(persona="你是「x」", skills={})
        self.assertIsInstance(brain._call("x"), list)   # 正常路径不抛异常

        self.assertEqual(brain._parse("抱歉，我无法回答。"), [])
        self.assertEqual(brain._parse(""), [])
        self.assertEqual(brain._parse('{"not": "a list"}'), [])

        fenced = '```json\n[{"to":["a"],"kind":"QUERY","topic":"t","content":"c"}]\n```'
        parsed = brain._parse(fenced)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["to"], ["a"])

        chatty = '好的，我来执行：\n[{"to":["b"],"kind":"ANSWER","topic":"t","content":"ok"}]\n以上。'
        self.assertEqual(len(brain._parse(chatty)), 1)

        # 缺少 to 字段的指令应被丢弃，避免总线收到无法投递的消息
        self.assertEqual(brain._parse('[{"kind":"QUERY","topic":"t","content":"c"}]'), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
