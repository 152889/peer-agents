"""跨语言互操作 —— 真的起一个 Node 进程，让它当 Agent 参与协作。

这个文件存在的理由是：**纸面可移植不算可移植。**

``test_wire.py`` 和 ``test_remote.py`` 都用 Python 扮演对端，所以它们只能
证明「协议能被自己实现对」。要证明它真的跨语言，唯一诚实的办法是把
``tools/peer_agent.mjs`` 跑起来 —— 一个零 npm 依赖、只用 ``node:net`` 的
参考实现，和一个 Python 宿主完成握手、换信、结案。

关于「本机没装 node 怎么办」
---------------------------
这是一条**会悄悄失效的测试**：跳过它，整套仍然全绿，但跨语言这件事其实
已经没人验证了。所以这里加了一个开关 ——

    PEER_AGENTS_REQUIRE_NODE=1

设上它，找不到 node 就是**失败**而不是跳过。CI 会设这个变量，
这样「跨语言」这四个字在 CI 里永远有人盯着。

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer_agents import message as M
from peer_agents import transport as T
from peer_agents import wire as W
from peer_agents.agent import Agent, EchoBrain, RuleBrain
from peer_agents.blackboard import Blackboard
from peer_agents.bus import MessageBus, full_mesh_policy
from peer_agents.remote import attach
from peer_agents.runtime import QUIESCENT, Runtime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PEER_SCRIPT = os.path.join(ROOT, "tools", "peer_agent.mjs")
NODE = shutil.which("node")
REQUIRE_NODE = os.environ.get("PEER_AGENTS_REQUIRE_NODE") == "1"

# 刻意用中文 + 全角标点：UTF-8 一路不能被谁偷偷转义成 \uXXXX，
# 否则中文用户的私有信息会在线上变成一坨没人能读的东西。
FACT = "order_items 出现全表扫描（780ms），基线是索引扫描 12ms"
STARTUP_TIMEOUT = 25.0


def node_args(port: int, *extra: str) -> list[str]:
    return [
        NODE or "node",
        PEER_SCRIPT,
        "--host", "127.0.0.1",
        "--port", str(port),
        "--agent", "dba",
        "--role", "数据库团队",
        "--owns", "slow_query",
        "--fact", f"slow_query={FACT}",
        *extra,
    ]


class NodePeerRig:
    """宿主 + 一个真的 Node 对端。

    错误流进了临时文件而不是管道：管道满了会**把子进程卡死**，
    于是测试超时、而真正的原因（一行报错）还堵在缓冲区里没出来。
    """

    def __init__(self, *extra: str, expected: str = "dba") -> None:
        self.srv = T.WireServer().listen()
        # 临时文件本可以就地 open，但它的生命周期要跟本对象一致（构造失败、用例抛错
        # 都得能收尾），所以借 ExitStack 托管。pop_all() 把回调挪出 with 块，
        # 否则 with 一退文件就被关掉，子进程的 stderr 还没人读。
        with contextlib.ExitStack() as stack:
            self._err = stack.enter_context(
                tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace")
            )
            self._stack = stack.pop_all()
        self.proc = subprocess.Popen(
            node_args(self.srv.port, *extra),
            stdout=subprocess.DEVNULL, stderr=self._err, cwd=ROOT,
        )
        try:
            self.peer, self.hello = self.srv.accept(timeout=STARTUP_TIMEOUT, expect_agent=expected)
        except Exception:
            self.shutdown()
            raise
        T.send_welcome(
            self.peer, session="s1", agents=["boss", "dba", "sink"],
            directory={"slow_query": ["dba"]}, hub=None,
        )

    # -- 诊断 -------------------------------------------------------------
    def stderr_text(self) -> str:
        self._err.seek(0)
        return self._err.read()

    def finish_cleanly(self) -> int:
        """礼尚往来地收工，并断言对端是**自己走**的（返回码 0）。"""
        self.peer.say_bye()
        self.peer.close()
        code = self.proc.wait(timeout=STARTUP_TIMEOUT)
        self.srv.close()
        if code != 0:
            self.fail(f"对端非正常退出（{code}）：\n{self.stderr_text()}")
        return code

    def shutdown(self) -> None:
        # 清理阶段不该再掀翻测试，所以每一步各自 suppress：任何一步失败都不挡住
        # 后面的收尾，否则前一个异常会让进程/端口漏在路上。
        with contextlib.suppress(Exception):
            self.peer.say_bye()
            self.peer.close()
        with contextlib.suppress(Exception):
            self.proc.wait(timeout=5)
        if self.proc.poll() is None:
            self.proc.kill()
        with contextlib.suppress(Exception):
            self.srv.close()
        self._stack.close()

    def __enter__(self) -> NodePeerRig:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.shutdown()


# ---------------------------------------------------------------------------
class TestNodeAvailability(unittest.TestCase):
    def test_node_is_present_when_it_is_required(self):
        """CI 里设了开关就**不许**跳过 —— 否则这个文件会变成一具空壳。"""
        if REQUIRE_NODE:
            self.assertIsNotNone(NODE, "设了 PEER_AGENTS_REQUIRE_NODE=1，却找不到 node")
        self.assertTrue(os.path.isfile(PEER_SCRIPT), f"参考实现不存在：{PEER_SCRIPT}")


@unittest.skipUnless(NODE, "本机没有 node；设 PEER_AGENTS_REQUIRE_NODE=1 可把它变成硬失败")
class TestNodeInterop(unittest.TestCase):
    def test_handshake_carries_identity_and_scope(self):
        with NodePeerRig() as rig:
            self.assertEqual(rig.hello.agent, "dba")
            self.assertEqual(rig.hello.role, "数据库团队")
            self.assertEqual(rig.hello.owns, ("slow_query",))
            self.assertFalse(
                rig.hello.can_decide_links,
                "对端没声明能力时不能默认给它 —— 否则「声明」就变成了「申请什么给什么」",
            )
            self.assertEqual(rig.finish_cleanly(), 0)

    def test_the_reference_implementation_answers_a_think_frame(self):
        with NodePeerRig() as rig:
            query = M.Message(sender="boss", recipients=("dba",), kind=M.QUERY,
                              topic="slow_query", content="看看慢查询")
            rig.peer.send(W.make_think(1, "dba", "数据库团队", [query],
                                       {"slow_query": ["dba"]}, {}))
            result = W.parse_say(rig.peer.expect(W.F_SAY), peer_id="dba",
                                 known_agents={"boss", "dba"}, hub=None)
            self.assertEqual(len(result.speeches), 1)
            speech = result.speeches[0]
            self.assertEqual(speech.kind, M.ANSWER)
            self.assertEqual(speech.recipients, ("boss",), "答复必须回到提问者")
            self.assertEqual(speech.reply_to, query.msg_id, "寻址信息要跟着消息走")
            self.assertEqual(speech.content, FACT, "UTF-8 必须一路原样回来")
            self.assertEqual(rig.finish_cleanly(), 0)

    def test_the_reference_implementation_says_nothing_about_topics_it_does_not_own(self):
        """答所问，别无其他 —— 最小大脑不该顺手广播自己的私事。"""
        with NodePeerRig() as rig:
            other = M.Message(sender="boss", recipients=("dba",), kind=M.QUERY,
                              topic="release_meta", content="发布记录？")
            rig.peer.send(W.make_think(1, "dba", "数据库团队", [other], {}, {}))
            result = W.parse_say(rig.peer.expect(W.F_SAY), peer_id="dba",
                                 known_agents={"boss", "dba"}, hub=None)
            self.assertEqual(result.speeches, ())
            self.assertFalse(result.pending, "没有欠着的东西就不该拖住收敛")
            self.assertEqual(rig.finish_cleanly(), 0)

    def test_a_node_brain_drives_a_real_run(self):
        """把 Node 进程接成一个 Agent，让它把一次协作真正跑完。"""
        with NodePeerRig() as rig:
            bus = MessageBus(policy=full_mesh_policy)
            board = Blackboard()
            board.publish_directory({"slow_query": ["dba"]})

            boss = Agent("boss", "值班 SRE", RuleBrain(
                owns=("final_report",),
                hypothesis=({"slow_query"}, "final_report", "【事故根因报告】{slow_query}"),
                report_to="sink",
            ), bus)
            dba, _brain = attach(bus, rig.peer, rig.hello, session="s1")
            sink = Agent("sink", "归档", EchoBrain(), bus)

            rt = Runtime([boss, dba, sink], bus, board, max_rounds=12)
            rt.bus.send(M.Message(sender="boss", recipients=("dba",), kind=M.QUERY,
                                  topic="slow_query", content="看看慢查询"))
            r = rt.run("node-interop", ground_truth_tokens=["全表扫描", "780ms"])

            self.assertTrue(r.completed, "Node 侧答复到位，Python 侧就该结案")
            self.assertTrue(r.ok)
            self.assertEqual(r.exit_reason, QUIESCENT)
            self.assertEqual(r.faults, [], "跨语言不该引入额外失败")
            self.assertEqual(r.fidelity, 1.0)
            self.assertIn("全表扫描", r.final_report)
            self.assertEqual(rig.finish_cleanly(), 0)

    def test_the_peer_decides_links_once_it_declares_the_capability(self):
        """要么按自己声明的能力表态，要么就别声明 —— 中间地带不存在。"""
        with NodePeerRig("--decide-links") as rig:
            self.assertTrue(rig.hello.can_decide_links)

            rig.peer.send(W.make_link_ask("boss", "slow_query", M.QUERY))
            self.assertTrue(W.parse_link_answer(rig.peer.expect(W.F_LINK_ANSWER)))

            rig.peer.send(W.make_link_ask("boss", "release_meta", M.QUERY))
            self.assertFalse(W.parse_link_answer(rig.peer.expect(W.F_LINK_ANSWER)))

            # 对方在向我汇报 —— 这种链必须接，否则结论回不来
            rig.peer.send(W.make_link_ask("deploy", "whatever", M.FINAL))
            self.assertTrue(W.parse_link_answer(rig.peer.expect(W.F_LINK_ANSWER)))

            self.assertEqual(rig.finish_cleanly(), 0)

    def test_the_reference_implementation_fails_loudly_on_an_unknown_frame(self):
        """参考实现自己也得「错了就响」—— 它是给人抄的样板，不是能跑就行。"""
        with NodePeerRig() as rig:
            rig.peer.send({"t": "make_me_a_sandwich"})
            code = rig.proc.wait(timeout=STARTUP_TIMEOUT)
            self.assertEqual(code, 3, f"协议违规应以 3 退出，实际 {code}")
            self.assertIn("make_me_a_sandwich", rig.stderr_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
