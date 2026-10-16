"""场景：一次生产故障的分布式定位。

为什么选这个场景？
------------------
根因分散在 4 个团队的私有信息里，任何单个 Agent 都不掌握全貌：

    网关      只知道「不是我的问题，瓶颈在下游 DB 连接等待」
    数据库组  只知道「order_items 全表扫描」
    发布平台  只知道「v2.31 发布了，但没动 DDL」
    订单服务  只知道「v2.31 重写了查询谓词」
                        ↓ 拼起来才是
    真正根因：谓词变了 → 原索引失效 → SeqScan → 连接池打满 → P99 劣化

关键在于推理路径是**动态生成**的：DBA 只有在拿到「本次发布没有 DDL」之后，
才有理由去追问「那是不是查询语句变了」。这种 A 的结论决定 B 该问谁的链条，
正是中心化编排最难高效表达的，也是子 Agent 互相直连最大的价值。

把 ``publish_all_findings=True`` 打开，可以让 Agent 把中间结论也贴上共享黑板，
观察「间接通信」通道对协作效率的额外影响。
"""

from __future__ import annotations

from .agent import Agent, RuleBrain
from .blackboard import Blackboard
from .bus import MessageBus, full_mesh_policy, star_policy
from .runtime import Runtime
from .topology import Topology

# ---- topic 词表 ---------------------------------------------------------
T_SCOPE = "symptom_scope"      # 故障边界：是网关侧还是下游
T_SLOWQ = "slow_query"         # 慢查询实况
T_RELEASE = "release_meta"     # 发布元数据
T_QUERYCHG = "query_change"    # 查询语句变更
T_ROOTCAUSE = "root_cause"     # 根因结论
T_REPORT = "final_report"      # 最终报告

# ---- 参演阵容 -----------------------------------------------------------
_ROSTER = ["sre", "gateway", "dba", "deploy", "ordersvc"]
_TITLES = {
    "sre": "值班 SRE / 结案",
    "gateway": "API 网关",
    "dba": "数据库团队",
    "deploy": "发布平台",
    "ordersvc": "订单服务",
}

# 每个 Agent 最多持有的直连数。刻意设小，好让「链路是一种稀缺资源」
# 这件事在实验里真正体现出来 —— 预算满了就必须淘汰旧连接。
LINK_BUDGET = 3

DIRECTORY = {
    T_SCOPE: "gateway",
    T_SLOWQ: "dba",
    T_RELEASE: "deploy",
    T_QUERYCHG: "ordersvc",
    T_ROOTCAUSE: "dba",
    T_REPORT: "sre",
}

# 判定信息保真度用的关键 token：一旦枢纽转述时丢细节，这些词就会从终报里消失
GROUND_TRUTH_TOKENS = ["SeqScan", "idx_order_items_status", "status IN", "连接池"]


def build(
    topology: str = "peer",
    hub: str = "sre",
    hub_summarize_chars: int | None = None,
    publish_all_findings: bool = False,
    link_budget: int = LINK_BUDGET,
) -> tuple[Runtime, Blackboard]:
    """组装一个可运行的故障定位系统。

    topology:
        "peer"     对等全连通（静态 policy）
        "star"     星型，一切经 hub 转述（静态 policy）
        "adaptive" 动态拓扑 —— 开局是一根脆弱的链，链路由 Agent 自行协商长出
    hub_summarize_chars: 枢纽代投时的截断长度，用来模拟「主 Agent 转述丢细节」
    publish_all_findings: 是否把中间结论也贴上共享黑板
    link_budget: 每个 Agent 最多持有的直连数（仅 adaptive 模式生效）
    """
    active_hub = hub if topology == "star" else None
    board = Blackboard()
    board.publish_directory(DIRECTORY)
    brain_map = _build_brains(active_hub, hub_summarize_chars)

    if topology == "peer":
        bus = MessageBus(policy=full_mesh_policy)
    elif topology == "adaptive":
        # 开局：sre–gateway–deploy–ordersvc 一条脆弱的链，而数据库组是孤岛。
        # 谁能连上谁，完全交给 Agent 自己谈。
        topo = Topology(
            nodes=list(_ROSTER),
            budget=link_budget,
            initial_links=[("sre", "gateway"), ("gateway", "deploy"), ("deploy", "ordersvc")],
        )

        def accept(owner_id: str, requester_id: str, msg) -> bool:  # type: ignore[no-untyped-def]
            br = brain_map.get(owner_id)
            return bool(br and br.accepts_link(requester_id, msg))

        bus = MessageBus(topology=topo, accept_hook=accept)
    else:
        bus = MessageBus(policy=star_policy(hub), hub=hub)

    agents = [
        Agent(_ROSTER[i], _TITLES[_ROSTER[i]], brain_map[_ROSTER[i]], bus)
        for i in range(len(_ROSTER))
    ]
    bus.register(*[a.id for a in agents])

    if publish_all_findings:
        for aid, br in brain_map.items():
            _enable_blackboard_publishing(aid, br, board)

    return Runtime(agents, bus, board, hub=active_hub), board


def _build_brains(active_hub: str | None, hub_summarize_chars: int | None) -> dict[str, RuleBrain]:
    return {
        "sre": RuleBrain(
            owns=(T_REPORT,),
            hypothesis=(
                {T_ROOTCAUSE},
                T_REPORT,
                "【事故根因报告】checkout 接口 P99 由 120ms 劣化至 900ms。{root_cause}",
            ),
            summarize_chars=hub_summarize_chars if active_hub == "sre" else None,
        ),
        "gateway": RuleBrain(
            owns=(T_SCOPE,),
            facts={
                T_SCOPE: "网关自身 P99 仅 8ms，外部 QPS 稳定在 1200/s；瓶颈在下游数据库连接等待（320ms）。"
            },
            triggers=((T_SCOPE, T_SLOWQ),),
            report_to="sre",
        ),
        "dba": RuleBrain(
            owns=(T_SLOWQ,),
            facts={
                T_SLOWQ: "order_items 出现全表扫描，单次耗时 780ms（基线为索引扫描 12ms）。"
            },
            triggers=((T_SLOWQ, T_RELEASE), (T_RELEASE, T_QUERYCHG)),
            hypothesis=(
                {T_SLOWQ, T_RELEASE, T_QUERYCHG},
                T_ROOTCAUSE,
                "{slow_query} 经核实，{release_meta} 而 {query_change} "
                "→ 结论：索引与谓词失配导致 SeqScan，连接池被打满。",
            ),
            report_to="sre",
        ),
        "deploy": RuleBrain(
            owns=(T_RELEASE,),
            facts={
                T_RELEASE: "v2.31 于昨日 14:00 由订单服务发布，本次发布不含任何 DDL 或索引变更。"
            },
        ),
        "ordersvc": RuleBrain(
            owns=(T_QUERYCHG,),
            facts={
                T_QUERYCHG: "v2.31 将下单查询的 WHERE 谓词由 status='NEW' 重写为 status IN (6 个值)，"
                "原 idx_order_items_status 单列索引不再匹配新谓词。"
            },
        ),
    }


def kickoff(board: Blackboard) -> None:
    """由 SRE 发起一次故障定位任务（交给网关，因为症状最先在网关露出）。"""
    board.artifacts.setdefault("trigger", "checkout P99 劣化告警")


def _enable_blackboard_publishing(agent_id: str, brain: RuleBrain, board: Blackboard) -> None:
    """可选增强：Agent 把答出的结论同步贴上共享黑板。

    这样其他人不必再问一遍，实现「一次查清、全局复用」的间接协作。
    """
    original = brain._maybe_answer  # noqa: SLF001 - 演示用，故意直接改写

    def wrapper(agent: Agent, m):  # type: ignore[no-untyped-def]
        reply = original(agent, m)
        if reply is not None:
            board.publish(reply.topic, reply.content, agent.id)
        return reply

    brain._maybe_answer = wrapper  # type: ignore[method-assign]  # noqa: SLF001
