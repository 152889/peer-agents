"""远端大脑与传输层的回归测试。

这一层要证明的是：**一个别的进程，可以真的驱动这套框架。**

关键工程决定是「一个线程都不用」。做法有两处：

  * ``socket.socketpair()`` 给出一对已连通的套接字，两端可以在同一个线程里
    交替使用；
  * TCP 握手利用一个内核事实：连接在 ``accept()`` 被调用之前就排进队列了，
    所以「先 connect、再 accept」在同线程里不会卡住。

为什么这么较真：README §5 把「单线程轮转、没有并发、竞态未测」列为一条
**未验证边界**。顺手在测试里开几个线程，就等于在这条边界上悄悄盖了房子。

``RemoteBrain`` 的用例里有个固定套路：**先把答复推进缓冲区，再调用 think**。
因为 think 是同步往返，先写后读能拿到一样的结果，而 socket 缓冲区替我们
排了队 —— 于是同步的请求/响应也能在单线程里被测。真实的收发次序由
``test_interop.py`` 里那个真跑起来的 Node 进程负责验证。

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import socket
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M
from peer_agents import transport as T
from peer_agents import wire as W
from peer_agents.agent import Agent, EchoBrain, RuleBrain
from peer_agents.blackboard import Blackboard
from peer_agents.bus import MessageBus, full_mesh_policy
from peer_agents.remote import RemoteBrain, attach
from peer_agents.runtime import QUIESCENT, Runtime

FAST = 1.0


def hello(agent: str = "dba", owns: tuple[str, ...] = ("slow_query",),
          caps: tuple[str, ...] = ()) -> W.Hello:
    return W.Hello(proto=W.PROTO_VERSION, agent=agent, role="数据库团队", owns=owns, caps=caps)


def answer_frame(content: str = "order_items 出现全表扫描", to: str = "sre",
                 topic: str = "slow_query", pending: object = None) -> dict:
    frame = {"t": W.F_SAY, "msgs": [{"to": [to], "kind": M.ANSWER,
                                     "topic": topic, "content": content}]}
    if pending is not None:
        frame["pending"] = pending
    return frame


class SocketCase(unittest.TestCase):
    """统一关 socket。

    泄漏的 fd 不会马上报错，它会等到某一轮跑多了之后变成一次**假失败** ——
    而假失败比真失败贵得多。
    """

    def setUp(self) -> None:
        self._closeables: list[object] = []

    def tearDown(self) -> None:
        for item in self._closeables:
            close = getattr(item, "close", None)
            if close is not None:
                close()

    def track(self, *items):
        self._closeables.extend(items)
        return items[0] if len(items) == 1 else items

    def pair(self) -> tuple[T.Peer, T.Peer]:
        """宿主侧与「假远端」各一端，可同线程交替使用。"""
        a, b = socket.socketpair()
        return self.track(T.Peer(a, default_timeout=FAST), T.Peer(b, default_timeout=FAST))

    def with_brain(self, brain: object, aid: str = "dba") -> tuple[Agent, MessageBus]:
        bus = MessageBus(policy=full_mesh_policy)
        return Agent(aid, "数据库团队", brain, bus), bus  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
class TestPeerTransport(SocketCase):
    def test_frame_round_trip(self):
        a, b = self.pair()
        a.send({"t": W.F_BYE, "note": "中文"})
        self.assertEqual(b.expect(W.F_BYE)["note"], "中文")

    def test_a_silent_peer_times_out_instead_of_hanging_forever(self):
        """沉默的对端不该有能力把调度器永久挂住。"""
        _a, b = self.pair()
        b.default_timeout = 0.05
        with self.assertRaises(TimeoutError):
            b.recv()
        self.assertFalse(b.broken, "超时是暂时的，不能等同于链路坏掉")

    def test_error_frame_becomes_a_wire_error_with_its_reason(self):
        """对端说了为什么拒收 —— 这一层要把它翻成人话，而不是让人去猜。"""
        a, b = self.pair()
        a.send(W.make_error("to 必须是数组"))
        with self.assertRaises(W.WireError) as ctx:
            b.recv()
        self.assertIn("to 必须是数组", str(ctx.exception))
        self.assertFalse(b.broken, "对端拒了一帧不等于链路断了")

    def test_wrong_frame_type_is_reported_with_both_names(self):
        a, b = self.pair()
        a.send({"t": W.F_BYE})
        with self.assertRaises(W.MalformedFrame) as ctx:
            b.expect(W.F_SAY)
        self.assertIn("say", str(ctx.exception))
        self.assertIn("bye", str(ctx.exception))

    def test_clean_eof_returns_none_and_marks_the_link_broken(self):
        a, b = self.pair()
        a.sock.shutdown(socket.SHUT_WR)
        self.assertIsNone(b.recv())
        self.assertTrue(b.broken, "对端走了 —— 这是「链路坏了」，不是「这轮没赶上」")

    def test_blank_lines_are_heartbeats_not_frames(self):
        a, b = self.pair()
        a.sock.sendall(b"\n   \n\n")
        a.send({"t": W.F_BYE})
        self.assertEqual(b.expect(W.F_BYE)["t"], "bye")

    def test_half_a_line_followed_by_eof_is_malformed(self):
        """半截报文不能被当成「什么都没收到」悄悄咽掉。"""
        a, b = self.pair()
        a.sock.sendall(b'{"t":"say","msgs":')
        a.sock.shutdown(socket.SHUT_WR)
        with self.assertRaises(W.MalformedFrame):
            b.recv()

    def test_a_reader_can_be_capped(self):
        """无界的缓冲就是泄漏（§9 第 19 条）—— 读取器必须敢在读完之前就报错。"""
        a, b = self.pair()
        reader = T.FrameReader(b.sock, max_line=64)
        a.sock.sendall(b"x" * 200)
        with self.assertRaises(W.FrameTooLarge):
            reader.readline()

    def test_sending_after_close_is_refused(self):
        a, _b = self.pair()
        a.close()
        with self.assertRaises(W.WireError):
            a.send({"t": W.F_BYE})


# ---------------------------------------------------------------------------
class TestTcpHandshake(SocketCase):
    """握手 —— 顺带证明「远端接进来」这件事不需要线程。"""

    def _open(self, payload: dict | None = None) -> tuple[T.WireServer, T.Peer]:
        srv = self.track(T.WireServer().listen())
        client = self.track(T.open_connection(srv.host, srv.port, timeout=FAST))
        client.send(payload if payload is not None else W.make_hello(
            "dba", "数据库团队", ("slow_query",)))
        return srv, client

    def test_two_step_handshake_needs_no_threads(self):
        srv, client = self._open()
        peer, hello_obj = srv.accept(timeout=FAST, expect_agent="dba")
        self.track(peer)
        T.send_welcome(peer, session="s1", agents=["sre", "dba"],
                       directory={"slow_query": ["dba"]}, hub=None)
        welcome = client.expect(W.F_WELCOME)
        self.assertEqual(hello_obj.agent, "dba")
        self.assertEqual(hello_obj.owns, ("slow_query",))
        self.assertEqual(welcome["proto"], W.PROTO_VERSION)
        self.assertEqual(welcome["agents"], ["sre", "dba"])
        self.assertEqual(welcome["directory"], {"slow_query": ["dba"]})
        self.assertIsNone(welcome["hub"])

    def test_the_seat_determines_who_you_are(self):
        """身份在会话里就定死了 —— 对端不能自己挑一个名字坐进来。"""
        srv, client = self._open(W.make_hello("sre", "值班 SRE"))
        with self.assertRaises(W.PermissionDenied):
            srv.accept(timeout=FAST, expect_agent="dba")
        frame = client.reader.readline()          # 绕过翻译，看原始帧
        self.assertEqual(frame["t"], W.F_ERROR)
        self.assertIn("座位", frame["reason"])

    def test_version_mismatch_is_refused_over_the_wire_too(self):
        payload = W.make_hello("dba")
        payload["proto"] = W.PROTO_VERSION + 41
        srv, client = self._open(payload)
        with self.assertRaises(W.VersionMismatch):
            srv.accept(timeout=FAST, expect_agent="dba")
        frame = client.reader.readline()
        self.assertEqual(frame["t"], W.F_ERROR)
        self.assertIn(str(W.PROTO_VERSION + 41), frame["reason"])

    def test_a_talking_peer_is_still_a_real_agent_on_the_bus(self):
        srv, client = self._open()
        peer, hello_obj = srv.accept(timeout=FAST, expect_agent="dba")
        self.track(peer)
        T.send_welcome(peer, session="s1", agents=["sre", "dba"], directory={}, hub=None)
        client.expect(W.F_WELCOME)

        bus = MessageBus(policy=full_mesh_policy)
        agent, brain = attach(bus, peer, hello_obj, session="s1")
        self.assertIn("dba", bus.boxes, "接进来的远端必须出现在总线的名册里")
        self.assertEqual(agent.id, "dba")
        self.assertIs(agent.brain, brain)
        self.assertTrue(callable(getattr(brain, "hangup", None)))

    def test_bye_is_polite_and_survives_a_dead_peer(self):
        srv, _client = self._open()
        peer, _hello_obj = srv.accept(timeout=FAST, expect_agent="dba")
        self.track(peer)
        peer.close()
        peer.say_bye()                            # 不该抛
        self.assertTrue(peer.closed)


# ---------------------------------------------------------------------------
class TestRemoteBrainProtocol(SocketCase):
    def _rig(self, *frames: dict, owns: tuple[str, ...] = ("slow_query",),
             caps: tuple[str, ...] = ()) -> tuple[RemoteBrain, T.Peer, Agent, MessageBus]:
        host, fake = self.pair()
        for frame in frames:
            fake.send(frame)
        brain = RemoteBrain(host, hello(owns=owns, caps=caps))
        agent, bus = self.with_brain(brain)
        return brain, fake, agent, bus

    def test_a_remote_answer_lands_as_a_real_message(self):
        brain, fake, agent, _bus = self._rig(answer_frame("order_items 全表扫描"))
        out = brain.think(agent, [], Blackboard())
        self.assertEqual(len(out), 1)
        msg = out[0]
        self.assertEqual(msg.kind, M.ANSWER)
        self.assertEqual(msg.topic, "slow_query")
        self.assertEqual(msg.content, "order_items 全表扫描")
        self.assertEqual(msg.sender, "dba", "身份由宿主填，不听对端的")
        self.assertNotEqual(msg.msg_id, "", "编号必须由框架发")
        frame = fake.expect(W.F_THINK)
        self.assertEqual(frame["me"]["id"], "dba")

    def test_the_remote_sees_its_mailbox_and_the_blackboard(self):
        brain, fake, agent, _bus = self._rig(answer_frame())
        board = Blackboard()
        board.publish_directory({"slow_query": ["dba"]})
        board.publish("release_meta", "v2.31 无 DDL", "deploy")
        incoming = M.Message(sender="sre", recipients=("dba",), kind=M.QUERY,
                             topic="slow_query", content="在吗")
        brain.think(agent, [incoming], board)
        frame = fake.expect(W.F_THINK)
        self.assertEqual(frame["box"][0]["from"], "sre")
        self.assertEqual(frame["box"][0]["topic"], "slow_query")
        self.assertEqual(frame["directory"], {"slow_query": ["dba"]})
        self.assertEqual(frame["facts"], {"release_meta": "v2.31 无 DDL"})

    def test_privilege_escalation_is_not_swallowed(self):
        """远端想发控制面报文 —— 必须一路抛到底，被框架记成故障。"""
        bad = {"t": W.F_SAY, "msgs": [{"to": ["sre"], "kind": M.LINK_REQUEST,
                                       "topic": "t", "content": "c"}]}
        brain, _fake, agent, _bus = self._rig(bad)
        with self.assertRaises(W.PermissionDenied):
            brain.think(agent, [], Blackboard())

    def test_forged_relayed_by_is_not_swallowed(self):
        bad = {"t": W.F_SAY, "msgs": [{"to": ["sre"], "kind": M.ANSWER,
                                       "topic": "t", "content": "c", "relayed_by": "sre"}]}
        brain, _fake, agent, _bus = self._rig(bad)
        with self.assertRaises(W.PermissionDenied):
            brain.think(agent, [], Blackboard())

    def test_a_dead_peer_makes_think_raise_rather_than_say_nothing(self):
        """不自己兜异常 —— 抛出去交给 Runtime 的故障隔离，失败才会可见。"""
        host, fake = self.pair()
        brain = RemoteBrain(host, hello())
        agent, _bus = self.with_brain(brain)
        fake.close()
        with self.assertRaises((W.WireError, OSError)):
            brain.think(agent, [], Blackboard())

    def test_pending_declaration_reaches_the_scheduler(self):
        brain, _fake, agent, _bus = self._rig(answer_frame(pending=True))
        brain.think(agent, [], Blackboard())
        self.assertTrue(brain.has_pending_work(Blackboard()))

        quiet, _fake2, agent2, _bus2 = self._rig(answer_frame(pending=False))
        quiet.think(agent2, [], Blackboard())
        self.assertFalse(quiet.has_pending_work(Blackboard()))


# ---------------------------------------------------------------------------
class TestRemoteLinkPolicy(SocketCase):
    """建链表态：对端声明了能力就由它自己表态，没声明就按默认策略代答。"""

    def _brain(self, caps: tuple[str, ...], *, reply: dict | None = None):
        host, fake = self.pair()
        if reply is not None:
            fake.send(reply)
        return RemoteBrain(host, hello(caps=caps)), fake

    def test_without_the_capability_the_host_applies_the_default_policy(self):
        brain, _fake = self._brain(caps=())
        asked = M.Message(sender="sre", recipients=("dba",), kind=M.QUERY,
                          topic="slow_query", content="在吗")
        self.assertTrue(brain.accepts_link("sre", asked), "问的是我负责的领域，该接")
        unrelated = asked.clone(topic="release_meta")
        self.assertFalse(brain.accepts_link("deploy", unrelated), "其余的别来稠密化拓扑")
        self.assertEqual(brain.link_asks, 0, "没声明能力就不该往线上发问")

    def test_an_answer_being_relayed_is_always_accepted(self):
        brain, _fake = self._brain(caps=())
        reporting = M.Message(sender="dba", recipients=("sre",), kind=M.ANSWER,
                              topic="whatever", content="结论")
        self.assertTrue(brain.accepts_link("dba", reporting))

    def test_a_declared_capability_really_asks_the_peer(self):
        brain, fake = self._brain(caps=(W.CAP_LINK,), reply={"t": W.F_LINK_ANSWER, "ok": True})
        msg = M.Message(sender="sre", recipients=("dba",), kind=M.QUERY,
                        topic="slow_query", content="在吗")
        self.assertTrue(brain.accepts_link("sre", msg))
        frame = fake.expect(W.F_LINK_ASK)
        self.assertEqual(frame["from"], "sre")
        self.assertEqual(frame["topic"], "slow_query")
        self.assertEqual(frame["origin_kind"], M.QUERY)
        self.assertEqual(brain.link_asks, 1)

    def test_the_peer_may_say_no(self):
        brain, _fake = self._brain(caps=(W.CAP_LINK,), reply={"t": W.F_LINK_ANSWER, "ok": False})
        msg = M.Message(sender="sre", recipients=("dba",), kind=M.QUERY, topic="t", content="c")
        self.assertFalse(brain.accepts_link("sre", msg))

    def test_a_broken_negotiation_is_a_rejection_not_a_crash(self):
        """谈不下来就不建链 —— 这是**合法且可见**的结果，不该掀翻整轮。

        宿主侧对这条路径没有故障隔离（§9 第 22 条），所以这里必须自己收住。
        它不会消失：链拒会计入 ``bus.link_rejects``，消息则进入下一轮重试，
        最终以 ``unreachable-after-negotiation`` 出现在 ``dropped`` 里。
        """
        host, fake = self.pair()
        brain = RemoteBrain(host, hello(caps=(W.CAP_LINK,)))
        fake.close()
        msg = M.Message(sender="sre", recipients=("dba",), kind=M.QUERY, topic="t", content="c")
        self.assertFalse(brain.accepts_link("sre", msg))
        self.assertEqual(brain.link_errors, 1)


# ---------------------------------------------------------------------------
class TestRemoteDrivesTheFramework(SocketCase):
    """端到端：一个远端大脑真的把一次协作跑完了。"""

    def _rig(self, *, repeats: int = 4, max_faults: int = 3) -> tuple[Runtime, RemoteBrain, T.Peer]:
        host, fake = self.pair()
        for _ in range(repeats):
            fake.send(answer_frame("order_items 出现全表扫描，单次耗时 780ms", to="boss"))
        brain = RemoteBrain(host, hello(owns=("slow_query",)))
        bus = MessageBus(policy=full_mesh_policy)
        board = Blackboard()
        board.publish_directory({"slow_query": ["dba"]})

        boss = Agent("boss", "值班 SRE", RuleBrain(
            owns=("final_report",),
            hypothesis=({"slow_query"}, "final_report", "【事故根因报告】{slow_query}"),
            report_to="sink",
        ), bus)
        dba = Agent("dba", "数据库团队", brain, bus)
        sink = Agent("sink", "归档", EchoBrain(), bus)

        rt = Runtime([boss, dba, sink], bus, board, max_rounds=12,
                     max_faults_per_agent=max_faults)
        rt.bus.send(M.Message(sender="boss", recipients=("dba",), kind=M.QUERY,
                              topic="slow_query", content="看看慢查询"))
        return rt, brain, fake

    def test_the_remote_answer_reaches_the_final_report(self):
        rt, brain, _fake = self._rig()
        r = rt.run("wire", ground_truth_tokens=["order_items", "全表扫描"])

        self.assertTrue(r.completed, "远端答复到位，本地大脑就该结案")
        self.assertTrue(r.ok)
        self.assertEqual(r.exit_reason, QUIESCENT)
        self.assertIn("order_items", r.final_report)
        self.assertEqual(r.fidelity, 1.0)
        self.assertEqual(r.faults, [], "正常路径不该产生任何故障")
        self.assertEqual(brain.rounds, 1, "远端只该被问一次（一个答复就够了）")
        self.assertEqual(brain.link_errors, 0, "这条路径上不该有建链异常")

    def test_the_remote_message_carries_the_frameworks_identity(self):
        rt, _brain, _fake = self._rig()
        r = rt.run("wire")
        from_dba = [m for m in r.messages if m.sender == "dba"]
        self.assertTrue(from_dba, "远端说的话必须作为 dba 进入审计轨迹")
        self.assertEqual(from_dba[0].msg_id, "m00001", "编号由框架发，且可复现")

    def test_a_dead_remote_is_isolated_evicted_and_reported(self):
        """远端进程没了 —— 该被摘除并说清楚，而不是把整场实验带走。"""
        rt, _brain, fake = self._rig(max_faults=2)
        fake.close()
        r = rt.run("dead-remote")            # 不抛异常 = 已隔离

        self.assertTrue(r.faults, "远端失效必须留下故障记录")
        self.assertIn("dba", r.faults[0]["agent"])
        self.assertIn("dba", r.describe_outcome())
        dba = next(a for a in rt.agents if a.id == "dba")
        self.assertFalse(dba.alive, "反复失联的远端应被摘除，不再拖住会话")
        self.assertTrue(r.dead_letters, "在途消息必须进死信队列，不能凭空蒸发")
        self.assertFalse(r.completed, "拿不到答复就不该假装结案")
        self.assertIn("slow_query", " ".join(v for items in r.unresolved.values() for v in items))


if __name__ == "__main__":
    unittest.main(verbosity=2)
