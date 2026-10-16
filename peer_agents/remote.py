"""把一个远端进程接成 Agent —— 它只负责「想」，别的都碰不到。

为什么是「大脑」而不是「Agent」
-------------------------------
这是整个跨语言设计里唯一重要的一个决定。

把远端做成一个完整 Agent（有自己的总线副本、自己维护拓扑），听起来更
「对等」，实际上会把项目的每一条结论都变成不可验证的：两个总线各自维护
拓扑，谁先谁后？去重表按谁算？「两次运行可逐字节 diff」还成立吗？

做成**大脑**则相反 —— 远端只是换了个地方思考，框架的一切约束原封不动：
  * 它发不出控制面报文（线协议拦一次，``MessageBus.send`` 再拦一次）；
  * 它无法修改拓扑（拓扑在宿主这边，由总线按需协商）；
  * 它不能决定自己是谁（``sender`` 由宿主按会话身份填）；
  * 它说了什么不重要，**框架决定留下什么**（§9 第 4 条的教训）。

一句话：远端继承的权限，和 ``LLMBrain`` 一模一样。

错误交给框架，不自己兜
----------------------
``think`` 里不写 ``try/except``。抛出去之后 Runtime 已经准备好接住它：
原因进 ``RunResult.faults``、在途报文进死信队列、连续出错达阈值就摘除
这个成员。在这里再兜一层，只会把一次「远端已经坏了」变成一次「远端
这轮什么都没说」—— 而后者正是 §9 花了一整节在追杀的那类静默失败。

唯一的例外是 ``accepts_link``：它被总线在投递过程中同步调用，
抛异常会直接掀翻整轮（宿主侧对这条路径没有故障隔离，见 §9 第 22 条）。
而「这次不建链」本身就是一个合法且**可见**的结果 —— 它会记进
``bus.link_rejects`` 与审计轨迹，消息则进入下一轮的挂起重试，
最终以 ``unreachable-after-negotiation`` 出现在 ``dropped`` 里。
"""

from __future__ import annotations

from .agent import Agent
from .blackboard import Blackboard
from .bus import MessageBus
from .message import ANSWER, FINAL, Message
from .transport import Peer
from .wire import (
    F_LINK_ANSWER,
    F_SAY,
    Hello,
    WireError,
    make_link_ask,
    make_think,
    parse_link_answer,
    parse_say,
)

# 每轮给远端看的黑板视图上限。黑板的职责是「共享状态」，
# 但把它整个原样推上线就等于开了一个会随运行时长增长的出口 ——
# 和 §9 第 19 条（无界的记忆就是泄漏）是同一件事。
MAX_FACTS_VIEW = 64
MAX_DIRECTORY_VIEW = 256


class RemoteBrain:
    """用一个 socket 上的对端来实现 ``Brain``。

    它同时满足 ``Brain`` 与 ``AdaptiveBrain`` 两个协议：``accepts_link``
    和 ``has_pending_work`` 是否**真的生效**，取决于对端在握手里有没有
    声明对应能力 —— 这和本地大脑用 ``getattr`` 探测是同一套思路，
    只是探测对象从「这个对象有没有这个方法」变成了「对端声明了没有」。
    """

    def __init__(self, peer: Peer, hello: Hello, *, session: str = "s1") -> None:
        self.peer = peer
        self.hello = hello
        self.session = session
        self.rounds = 0
        self.link_asks = 0
        self.link_errors = 0
        self._pending = False

    # -- Brain ------------------------------------------------------------
    def think(self, agent: Agent, batch: list[Message], board: Blackboard) -> list[Message]:
        self.rounds += 1
        self.peer.send(
            make_think(
                round_no=board.round,
                me_id=agent.id,
                me_role=agent.role,
                box=batch,
                directory=_directory_view(board),
                facts=_facts_view(board),
            )
        )
        reply = self.peer.expect(F_SAY)
        result = parse_say(
            reply,
            peer_id=agent.id,
            known_agents=set(agent.bus.boxes),
            hub=agent.bus.hub,
        )
        self._pending = result.pending
        return [speech.to_message(agent.id) for speech in result.speeches]

    # -- AdaptiveBrain（可选能力，按对端声明决定是否真的转发）---------------
    def accepts_link(self, requester: str, msg: Message) -> bool:
        if not self.hello.can_decide_links:
            return self._default_link_policy(msg)
        self.link_asks += 1
        try:
            self.peer.send(make_link_ask(requester, msg.topic, msg.origin_kind or msg.kind))
            reply = self.peer.expect(F_LINK_ANSWER)
            return parse_link_answer(reply)
        except (WireError, OSError):
            # 谈不下来就不建链。这是合法结果，而且可见（见模块 docstring）。
            self.link_errors += 1
            return False

    def has_pending_work(self, _board: Blackboard) -> bool:
        """我还欠着东西吗？

        除了对端自报的 ``pending``，还有一个只有宿主才看得见的情况：
        **链路已经坏了。** 此时这个成员身上所有的未了之事都注定交付不了，
        可它自己没法再说这句话（话都传不出来了）。

        所以坏掉的连接一律算「有未了之事」—— 于是它会被反复唤醒、反复记故障，
        达到阈值后按既有规则摘除。反过来，如果这里只报 ``False``，
        一个死掉的远端会安静地留在名册里，而它丢掉的那些消息再也没人提起。
        超时不走这条路：超时是暂时的，下一轮可能就好了。
        """
        return self._pending or self.peer.closed or self.peer.broken

    # -- 收尾 -------------------------------------------------------------
    def hangup(self) -> None:
        self.peer.say_bye()
        self.peer.close()

    def _default_link_policy(self, msg: Message) -> bool:
        """对端没声明建链表态时，宿主按 ``RuleBrain`` 的默认策略代答。

        和 ``RuleBrain.accepts_link`` 保持一致不是巧合：对端没表态时，
        它就该和一个默认配置的本地大脑待遇相同 —— 否则「远端」这个身份
        本身会变成一种隐藏特权。
        """
        if msg.topic in self.hello.owns:
            return True
        return msg.kind in (ANSWER, FINAL) or msg.origin_kind in (ANSWER, FINAL)


def attach(
    bus: MessageBus,
    peer: Peer,
    hello: Hello,
    *,
    session: str = "s1",
    role: str = "",
) -> tuple[Agent, RemoteBrain]:
    """把握手完成的对端接成总线上的一个 Agent。

    返回 ``(agent, brain)`` —— 把 brain 也交出来，是因为收尾时要 ``hangup()``，
    而且调用方有权知道自己这边记了多少次建链表态失败。
    """
    brain = RemoteBrain(peer, hello, session=session)
    agent = Agent(hello.agent, hello.role or role or hello.agent, brain, bus)
    return agent, brain


def _facts_view(board: Blackboard) -> dict[str, str]:
    items = list(board.facts.items())[:MAX_FACTS_VIEW]
    return {topic: content for topic, (content, _author) in items}


def _directory_view(board: Blackboard) -> dict[str, list[str]]:
    items = list(board.registry.items())[:MAX_DIRECTORY_VIEW]
    return {topic: list(owners) for topic, owners in items}


__all__ = ["MAX_DIRECTORY_VIEW", "MAX_FACTS_VIEW", "RemoteBrain", "attach"]
