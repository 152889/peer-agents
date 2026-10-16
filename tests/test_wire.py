"""线协议的回归测试。

这一层钉的**不是功能，是权限**。线协议是全项目唯一一处「外部进程能直接
往里灌数据」的地方，所以它的每条校验都对应一次真实的越权或误投 ——
而且每一条都能在 README §9 里找到出处：

    to 写成单个字符串         → 幽灵收件人（§9 第 11 条）
    kind 写成 LINK_REQUEST    → 绕开框架改写拓扑（§9 第 12 条）
    relayed_by 随手填         → 直接绕过拓扑投递（§9 第 1 条）
    relay_to 随手填           → 同上，而且能骗过枢纽去代投
    一个永不换行的巨行        → 无界内存（§9 第 19 条）

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M
from peer_agents import wire as W

ROSTER = {"sre", "gateway", "dba", "deploy", "ordersvc"}


def one(**kw) -> dict:
    base = {"to": ["sre"], "kind": "QUERY", "topic": "slow_query", "content": "在吗"}
    base.update(kw)
    return base


def say(items: list, *, peer_id: str = "dba", known: set[str] | None = None,
        hub: str | None = None, pending: object = None) -> W.SayResult:
    payload: dict = {"t": W.F_SAY, "msgs": items}
    if pending is not None:
        payload["pending"] = pending
    return W.parse_say(payload, peer_id=peer_id, known_agents=known or ROSTER, hub=hub)


# ---------------------------------------------------------------------------
class TestFraming(unittest.TestCase):
    """分帧：一行一帧，不合规的一律有明确归宿。"""

    def test_round_trip(self):
        payload = {"t": W.F_THINK, "round": 3, "content": "中文也要原样回来"}
        raw = W.encode_frame(payload)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertEqual(W.decode_line(raw.rstrip(b"\n")), payload)

    def test_encoding_is_human_readable(self):
        """可调试性是选 NDJSON 的全部理由 —— 所以中文不能被转义成 \\uXXXX。"""
        raw = W.encode_frame({"t": W.F_BYE, "note": "收工"})
        self.assertIn("收工".encode(), raw)

    def test_blank_line_is_not_a_frame(self):
        with self.assertRaises(W.MalformedFrame):
            W.decode_line(b"   ")

    def test_broken_json_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.decode_line(b'{"t": "say"')

    def test_non_object_frame_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.decode_line(b'["t", "say"]')

    def test_missing_frame_type_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.decode_line(json.dumps({"hello": 1}).encode())

    def test_unknown_frame_type_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.decode_line(json.dumps({"t": "sudo"}).encode())

    def test_invalid_utf8_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.decode_line(b"\xff\xfe\x00bad")

    def test_oversize_line_is_refused(self):
        with self.assertRaises(W.FrameTooLarge):
            W.decode_line(b"x" * (W.MAX_LINE_BYTES + 1))


# ---------------------------------------------------------------------------
class TestHandshake(unittest.TestCase):
    def test_hello_round_trip(self):
        hello = W.parse_hello(W.make_hello("dba", "数据库团队", ("slow_query",), (W.CAP_LINK,)))
        self.assertEqual(hello.agent, "dba")
        self.assertEqual(hello.owns, ("slow_query",))
        self.assertTrue(hello.can_decide_links)

    def test_version_mismatch_says_so_instead_of_limping_along(self):
        payload = W.make_hello("dba")
        payload["proto"] = 99
        with self.assertRaises(W.VersionMismatch) as ctx:
            W.parse_hello(payload)
        self.assertIn("99", str(ctx.exception))

    def test_bool_is_not_a_version_number(self):
        """Python 里 True == 1 —— 不显式排除布尔，一个 true 就能混过版本校验。"""
        payload = W.make_hello("dba")
        payload["proto"] = True
        with self.assertRaises(W.MalformedFrame):
            W.parse_hello(payload)

    def test_missing_version_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.parse_hello({"t": W.F_HELLO, "agent": "dba"})

    def test_empty_identity_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            W.parse_hello(W.make_hello(""))

    def test_unknown_capability_is_refused(self):
        """不认识的能力不能默认开启 —— 否则「声明」就变成了「申请什么给什么」。"""
        payload = W.make_hello("dba", caps=("accepts_link", "rewrite_topology"))
        with self.assertRaises(W.MalformedFrame) as ctx:
            W.parse_hello(payload)
        self.assertIn("rewrite_topology", str(ctx.exception))

    def test_caps_must_be_strings(self):
        payload = W.make_hello("dba")
        payload["caps"] = [1]
        with self.assertRaises(W.MalformedFrame):
            W.parse_hello(payload)


# ---------------------------------------------------------------------------
class TestRecipientAddressing(unittest.TestCase):
    """收件人必须已经是列表 —— **绝不替字符串猜意图**（§9 第 11 条）。"""

    def test_bare_string_recipient_is_refused_not_split(self):
        with self.assertRaises(W.MalformedFrame) as ctx:
            say([one(to="audit")])
        # 关键：它不是"被拆成 5 个幽灵收件人"，而是当场拒收
        self.assertIn("to", str(ctx.exception))

    def test_ghost_recipients_can_never_be_produced(self):
        """把 §9 第 11 条那次事故本身钉住：tuple("audit") 会炸成 a/u/d/i/t。"""
        with self.assertRaises(W.MalformedFrame):
            say([one(to="audit")])
        # 同一个输入在规则脑那侧确实是会炸的 —— 证明这个测试在防真事
        self.assertEqual(tuple("audit"), ("a", "u", "d", "i", "t"))

    def test_non_string_recipient_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(to=["sre", 7])])

    def test_empty_recipient_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(to=[""])])

    def test_empty_recipient_list_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(to=[])])

    def test_duplicate_recipients_are_refused(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(to=["sre", "sre"])])

    def test_broadcast_is_explicit_not_implicit(self):
        result = say([one(to=["*"], kind=M.INFORM)])
        self.assertEqual(result.speeches[0].recipients, ("*",))

    def test_multiple_recipients_survive_in_order(self):
        result = say([one(to=["sre", "gateway"])])
        self.assertEqual(result.speeches[0].recipients, ("sre", "gateway"))


# ---------------------------------------------------------------------------
class TestSpeechPermissions(unittest.TestCase):
    """这是整个模块的核心：远端只能决定「说什么」。"""

    def test_data_plane_kind_is_accepted(self):
        for kind in sorted(M.DATA_KINDS):
            with self.subTest(kind=kind):
                result = say([one(kind=kind)])
                self.assertEqual(result.speeches[0].kind, kind)

    def test_control_plane_kind_is_refused(self):
        for kind in sorted(M.CONTROL_KINDS):
            with self.subTest(kind=kind):
                with self.assertRaises(W.PermissionDenied) as ctx:
                    say([one(kind=kind)])
                self.assertIn("控制面", str(ctx.exception))

    def test_unknown_kind_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(kind="EXECUTE_ARBITRARY_CODE")])

    def test_framework_owned_fields_are_refused_loudly(self):
        """这些字段静默忽略不行 —— 对端会以为自己成功了。

        ``relayed_by`` 尤其危险：§9 第 1 条证明，只要它等于 hub，
        报文就直接绕过拓扑投递。让远端能填它，等于开了一条拓扑旁路。
        """
        for field, value in (("sender", "sre"), ("relayed_by", "sre"), ("origin_kind", "FINAL")):
            with self.subTest(field=field), self.assertRaises(W.PermissionDenied):
                say([one(**{field: value})])

    def test_redundant_fields_are_silently_overwritten(self):
        """框架自己会覆盖的字段，对端带上只是冗余（原样回传报文是常见写法）。"""
        result = say([one(id="m99999", msg_id="m99999", hop=7, path=["a", "b"], ts=1.0)])
        speech = result.speeches[0]
        M.reset_ids()
        msg = speech.to_message("dba")
        self.assertEqual(msg.msg_id, "m00000", "编号必须由框架发，不能听对端的")
        self.assertEqual(msg.hop, 0)
        self.assertEqual(msg.path, ())

    def test_unknown_extra_fields_are_ignored(self):
        result = say([one(whatever=1, nested={"a": [1, 2]})])
        self.assertEqual(len(result.speeches), 1)

    def test_sender_is_bound_to_the_session_identity(self):
        """远端不能决定自己是谁 —— 身份在握手时就绑死了。"""
        result = say([one()], peer_id="dba")
        self.assertEqual(result.speeches[0].to_message("dba").sender, "dba")

    def test_content_and_topic_must_be_strings(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(content=42)])
        with self.assertRaises(W.MalformedFrame):
            say([one(topic="")])
        with self.assertRaises(W.MalformedFrame):
            say([one(topic=None)])

    def test_reply_to_accepts_null_or_string(self):
        self.assertIsNone(say([one(reply_to=None)]).speeches[0].reply_to)
        self.assertEqual(say([one(reply_to="m00003")]).speeches[0].reply_to, "m00003")

    def test_reply_to_must_not_be_a_number(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(reply_to=3)])

    def test_msgs_must_be_a_list(self):
        with self.assertRaises(W.MalformedFrame):
            W.parse_say({"t": W.F_SAY, "msgs": {}}, peer_id="dba", known_agents=ROSTER, hub=None)

    def test_item_must_be_an_object(self):
        with self.assertRaises(W.MalformedFrame):
            say(["to: sre"])

    def test_batch_size_is_capped(self):
        with self.assertRaises(W.MalformedFrame):
            say([one()] * (W.MAX_BATCH + 1))

    def test_error_message_names_the_offending_item(self):
        with self.assertRaises(W.MalformedFrame) as ctx:
            say([one(), one(content={})])
        self.assertIn("msgs[1]", str(ctx.exception))


# ---------------------------------------------------------------------------
class TestRelayAuthority(unittest.TestCase):
    """代投会绕过拓扑 —— 只有枢纽身份的对端有权这么做（§9 第 1 条）。"""

    def test_non_hub_cannot_relay(self):
        with self.assertRaises(W.PermissionDenied) as ctx:
            say([one(relay_to="gateway")], peer_id="dba", hub="sre")
        self.assertIn("不是枢纽", str(ctx.exception))

    def test_nobody_can_relay_when_there_is_no_hub(self):
        with self.assertRaises(W.PermissionDenied):
            say([one(relay_to="gateway")], peer_id="sre", hub=None)

    def test_the_hub_may_relay(self):
        result = say([one(relay_to="gateway")], peer_id="sre", hub="sre")
        speech = result.speeches[0]
        self.assertEqual(speech.relay_to, "gateway")
        self.assertEqual(speech.relayed_by, "sre", "relayed_by 由身份推出，不取自线上")

    def test_the_hub_still_cannot_forge_relayed_by(self):
        """即便身份是枢纽，relayed_by 也必须由宿主推出来，不能听对端的。"""
        with self.assertRaises(W.PermissionDenied):
            say([one(relay_to="gateway", relayed_by="deploy")], peer_id="sre", hub="sre")

    def test_relaying_to_an_unknown_agent_is_refused(self):
        with self.assertRaises(W.MalformedFrame):
            say([one(relay_to="ghost")], peer_id="sre", hub="sre")

    def test_a_non_relaying_speech_has_no_relayed_by(self):
        speech = say([one()], peer_id="sre", hub="sre").speeches[0]
        self.assertIsNone(speech.relayed_by)


# ---------------------------------------------------------------------------
class TestPendingDeclaration(unittest.TestCase):
    """「我还欠着一个答复」必须能在线协议上表达。

    少了它，§9 第 7 条那次事故（系统在重试窗口打开之前就宣布收敛）
    会原样在远端 Agent 身上重演。
    """

    def test_defaults_to_false(self):
        self.assertFalse(say([one()]).pending)

    def test_true_is_carried_through(self):
        self.assertTrue(say([one()], pending=True).pending)

    def test_must_be_a_boolean(self):
        with self.assertRaises(W.MalformedFrame):
            say([one()], pending="yes")


# ---------------------------------------------------------------------------
class TestOutboundFrames(unittest.TestCase):
    def test_think_frame_shape(self):
        M.reset_ids()
        box = M.Message(sender="sre", recipients=("dba",), kind=M.QUERY,
                        topic="slow_query", content="在吗")
        frame = W.make_think(2, "dba", "数据库团队", [box], {"slow_query": ["dba"]}, {"t": "v"})
        self.assertEqual(frame["t"], W.F_THINK)
        self.assertEqual(frame["round"], 2)
        self.assertEqual(frame["me"], {"id": "dba", "role": "数据库团队"})
        self.assertEqual(frame["box"][0]["id"], "m00000", "远端要靠这个 id 才能 reply_to")
        self.assertEqual(frame["box"][0]["to"], ["dba"])
        self.assertNotIn("relay_to", frame["box"][0])

    def test_relay_to_is_visible_outbound(self):
        """对端可能是枢纽，得知道自己该代投什么。"""
        box = M.Message(sender="gateway", recipients=("sre",), kind=M.QUERY,
                        topic="t", content="c", relay_to="dba")
        frame = W.make_think(1, "sre", "SRE", [box], {}, {})
        self.assertEqual(frame["box"][0]["relay_to"], "dba")

    def test_link_ask_frame_shape(self):
        frame = W.make_link_ask("sre", "slow_query", "QUERY")
        self.assertEqual(frame["t"], W.F_LINK_ASK)
        self.assertEqual(frame["from"], "sre")
        self.assertEqual(frame["origin_kind"], "QUERY")

    def test_link_answer_requires_a_real_boolean(self):
        self.assertTrue(W.parse_link_answer({"t": W.F_LINK_ANSWER, "ok": True}))
        self.assertFalse(W.parse_link_answer({"t": W.F_LINK_ANSWER, "ok": False}))
        with self.assertRaises(W.MalformedFrame):
            W.parse_link_answer({"t": W.F_LINK_ANSWER, "ok": "yes"})

    def test_error_frame_carries_a_reason(self):
        self.assertIn("reason", W.make_error("to 必须是数组"))
        self.assertEqual(W.make_bye(), {"t": W.F_BYE})


if __name__ == "__main__":
    unittest.main(verbosity=2)
