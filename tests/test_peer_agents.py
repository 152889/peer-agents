"""回归测试：把 README 里每一条结论都钉死成可执行的断言。

只用标准库，不需要装任何依赖：

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M
from peer_agents.bus import MessageBus, star_policy
from peer_agents.scenario_incident import (
    GROUND_TRUTH_TOKENS,
    T_SCOPE,
    build,
)
from peer_agents.topology import Topology, edge_key

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
    runtime = build(**cfg)
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


class TestDeterminism(unittest.TestCase):
    """确定性回归：同一配置下，输出不该随进程变。

    ``Topology.links`` 是 ``dict[str, set[str]]`` —— set 的迭代顺序取决于字符串
    哈希，而 ``PYTHONHASHSEED`` 默认**每个进程随机**。凡是「迭代 set 再输出」
    的地方都会变成不确定的，其中 ``edges()`` 就漏了（``route()`` 一直是对的）。

    这类缺陷在同一个进程里几乎观测不到（进程内哈希稳定），必须**换进程、换种子**
    才暴露得出来 —— 所以下面用子进程而不是循环调用。
    """

    _PROBE = """
import json, sys
sys.path.insert(0, sys.argv[1])
from peer_agents.topology import Topology, edge_key

# 1) 链路枚举：给 a 挂 4 条直连，顺序差异才有机会显形
t = Topology(["a", "b", "c", "d", "e"], budget=5,
             initial_links=[("a", "b"), ("a", "c"), ("a", "d"), ("a", "e")])
edges = t.edges()

# 2) LRU 并列：人为把 a-b / a-c 的 last_used 写成同一个值
u = Topology(["a", "b", "c", "d"], budget=2, initial_links=[("a", "b")])
u.try_link("a", "c")
for peer in ("b", "c"):
    u.last_used[edge_key("a", peer)] = 7
u.try_link("a", "d")

print(json.dumps({
    "edges": edges,
    "kept": [p for p in ("b", "c") if u.has_link("a", p)],
    "evictions": u.evictions,
}))
"""

    def test_output_is_independent_of_hash_seed(self):
        """``edges()`` 的顺序、以及 LRU 并列时的淘汰结果，都不得随种子变。

        修复前：``edges()`` 直接迭代 ``self.links[a]``，6 个种子会打印出多种
        链路顺序，``run_demo.py`` 的「最终：」那一行每次不一样 —— 与 README
        宣称的「两次同配置运行可逐字节 diff」直接矛盾。
        """
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        runs: list[tuple[str, str]] = []
        for seed in ("0", "1", "2", "3", "4", "5"):
            proc = subprocess.run(
                [sys.executable, "-c", self._PROBE, root],
                capture_output=True,
                text=True,
                check=True,
                env={**os.environ, "PYTHONHASHSEED": seed},
            )
            runs.append((seed, proc.stdout.strip()))

        base_seed, base = runs[0]
        for seed, out in runs[1:]:
            with self.subTest(seed=seed):
                self.assertEqual(out, base, f"PYTHONHASHSEED={seed} 与 {base_seed} 的输出不同")

        payload = json.loads(base)
        self.assertEqual(
            [tuple(e) for e in payload["edges"]],
            [("a", "b"), ("a", "c"), ("a", "d"), ("a", "e")],
            "每条链路应只出现一次，且顺序确定",
        )
        self.assertEqual(payload["kept"], ["c"], "并列时应淘汰字典序更小的 a-b")
        self.assertEqual(payload["evictions"], 1)

    def test_eviction_tie_is_broken_by_peer_name(self):
        """``last_used`` 并列时按 peer 名字兜底，而不是按 set 迭代序。

        这条分支**目前走不到**：唯一的建链入口 ``force_link`` 建链后必定
        ``touch``，而 ``clock`` 严格递增，因此同一节点的邻居 ``last_used``
        必然互不相同（实测 budget=1/2/3 三档、共 10 次淘汰也没有出现并列）。

        所以这个测试是在**防未来**：一旦有人加了「不 touch 就建链」的路径，
        并列就会出现；届时淘汰结果必须仍然确定，否则拓扑演化与实验结论都
        不可复现 —— 那是比显示顺序严重得多的问题。
        """
        t = Topology(["a", "b", "c", "d"], budget=2, initial_links=[("a", "b")])
        t.try_link("a", "c")  # a: {b, c}，预算已满
        for peer in ("b", "c"):
            t.last_used[edge_key("a", peer)] = 7  # 人为制造并列
        t.try_link("a", "d")
        self.assertTrue(t.has_link("a", "c"), "并列时保留名字较大的 a-c")
        self.assertFalse(t.has_link("a", "b"), "并列时淘汰名字较小的 a-b")
        self.assertEqual(t.evictions, 1)


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
        for cfg in ({"topology": "peer"}, {"topology": "star"}, {"topology": "adaptive"}):
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
        runtime = build(topology="peer", brain_mode="llm")
        bootstrap(runtime)
        runtime.run(name="llm", ground_truth_tokens=GROUND_TRUTH_TOKENS)
        prompts = [req["prompt"] for a in runtime.agents
                   for req in getattr(a.brain, "requests", [])]
        self.assertTrue(prompts, "应当真实发出过请求")
        joined = "\n".join(prompts)
        for section in ("# 你的身份", "# 你掌握的私有信息", "# 团队共享黑板", "# 可用 topic", "# 输出格式", "# 本轮收到的消息"):
            self.assertIn(section, joined, f"prompt 缺少段落：{section}")
        self.assertIn("slow_query → 知情者 dba", joined, "prompt 里应带上通讯录")

    def test_request_is_well_formed(self):
        runtime = build(topology="peer", brain_mode="llm")
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

    def test_string_recipient_is_not_exploded_into_characters(self):
        """`"to": "audit"` 若被 tuple() 直接吃掉，会拆成 a/u/d/i/t 五个幽灵收件人，
        真正的 audit 一条都收不到 —— 没有异常、没有日志，只表现为「对方没回我」。"""
        from peer_agents.llm import LLMBrain

        parsed = LLMBrain._parse('[{"to":"audit","kind":"QUERY","topic":"t","content":"c"}]')
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["to"], ["audit"], "字符串收件人应被规整成单元素列表")

    def test_control_plane_kind_from_model_is_discarded(self):
        """模型不得伪造建链请求 —— 否则「不能越权改动框架」就是空话。"""
        from peer_agents.llm import LLMBrain

        for kind in ("LINK_REQUEST", "LINK_ACCEPT", "LINK_REJECT", "LINK_REVOKE"):
            with self.subTest(kind=kind):
                out = LLMBrain._parse(
                    f'[{{"to":["dba"],"kind":"{kind}","topic":"t","content":"c"}}]'
                )
                self.assertEqual(out, [], f"{kind} 属于控制面，必须丢弃")

    def test_unknown_kind_is_discarded(self):
        from peer_agents.llm import LLMBrain

        self.assertEqual(LLMBrain._parse('[{"to":["a"],"kind":"EVIL","topic":"t","content":"c"}]'), [])
        # 缺 kind 时退化为 INFORM，而不是丢弃 —— 这是常见的良性省略
        ok = LLMBrain._parse('[{"to":["a"],"topic":"t","content":"c"}]')
        self.assertEqual(ok[0]["kind"], "INFORM")

    def test_malformed_response_does_not_crash(self):
        """模型不听话、返回一堆废话时，应退化为「本轮不发消息」而不是崩掉。"""
        from peer_agents.llm import LLMBrain

        brain = LLMBrain(persona="你是「x」", skills={})
        self.assertIsInstance(brain._call("x"), list)   # 正常路径不抛异常

        self.assertEqual(brain._parse("抱歉，我无法回答。"), [])
        self.assertEqual(brain._parse(""), [])
        self.assertEqual(brain._parse('{"not": "a list"}'), [])
        self.assertEqual(brain._parse('["a","b"]'), [], "数组元素不是对象应丢弃")
        self.assertEqual(brain._parse("[1,2,3]"), [])
        self.assertEqual(brain._parse('[{"to":[],"kind":"QUERY","topic":"t","content":"c"}]'), [],
                         "空收件人应丢弃")

        fenced = '```json\n[{"to":["a"],"kind":"QUERY","topic":"t","content":"c"}]\n```'
        parsed = brain._parse(fenced)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["to"], ["a"])

        chatty = '好的，我来执行：\n[{"to":["b"],"kind":"ANSWER","topic":"t","content":"ok"}]\n以上。'
        self.assertEqual(len(brain._parse(chatty)), 1)

        # 缺少 to 字段的指令应被丢弃，避免总线收到无法投递的消息
        self.assertEqual(brain._parse('[{"kind":"QUERY","topic":"t","content":"c"}]'), [])

    def test_bus_refuses_control_plane_message_from_agent(self):
        """权限边界必须在总线上也守一道 —— 不能只靠大脑自觉。"""
        bus = MessageBus()
        bus.register("a", "b")
        for kind in ("LINK_REQUEST", "LINK_ACCEPT", "LINK_REJECT", "LINK_REVOKE"):
            with self.subTest(kind=kind):
                n = bus.send(M.Message(sender="a", recipients=("b",), kind=kind,
                                       topic="t", content="伪造的控制面报文"))
                self.assertEqual(n, 0, "Agent 不应能发送控制面报文")
        self.assertEqual(len(bus.boxes["b"]), 0)
        self.assertIn("control-plane-not-from-agent", {why for _m, _d, why in bus.dropped})

    def test_link_request_without_topology_does_not_crash(self):
        """原本这里是一句 assert —— 在 `python -O` 下会被剥掉，
        把一条本该优雅退场的分支变成生产环境才炸的地雷。"""
        bus = MessageBus()
        bus.register("a", "b")
        bus._deliver_control(
            M.Message(sender="a", recipients=("b",), kind=M.LINK_REQUEST, topic="t", content="x"), "b"
        )
        self.assertEqual(bus.link_requests, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
