"""「极度真实」的故障环境 —— 一个刻意做脏的场景。

与干净版场景（scenario_incident）的区别
--------------------------------------
干净版里每个 topic 只有一个知情者，信息永远新鲜，答案永远自洽。
于是「问对的人」就等于「拿到真相」。真实世界不是这样。

本场景往环境里注入了五种脏东西，每一种都针对性地质疑「对等一定更好」：

1. **一个 topic 多个知情者**（`index_state` / `release_meta` / `rate_limit`）。
   问一个和问全部，会得到**质量不同**的答案 —— 这就是交叉验证的落点。

2. **过期快照**。DBA 手上有一份 schema 快照，说「索引存在」，置信度 0.92。
   这是真话，但是 100 个时间单位之前的真话。
   它**没有错，它只是旧了** —— 而高置信度的旧信息，比低置信度的错信息更难识别。
   只校验置信度的 Agent 一定会踩中；必须同时校验新鲜度才拦得住。

3. **表面冲突**。发布平台说「本次无 DDL」，变更审计说「清单里有一条已回滚的 DDL」。
   两者都对，但只有交叉起来才能得出「索引没被删，是谓词变了」。

4. **干扰项（decoy）**。网关抛出一条低置信度传闻：「昨晚的限流变更可能是原因」。
   核实它的成本很低，但不核实就会把结论带偏 —— 这是给「轻信」准备的陷阱。

5. **不可靠成员**。订单服务正在处理线上问题，前两次提问一律不回。
   它逼出「重试」这个真实系统里必不可少、demo 里几乎从不出现的机制。

真值
----
谓词重写（`status IN`）→ 原单列索引不匹配 → SeqScan → 连接池打满。
而「限流变更」应当在核实后被明确排除。
"""

from __future__ import annotations

from .agent import Agent
from .blackboard import Blackboard
from .brain_dirty import GULLIBLE, SKEPTICAL, DirtyBrain
from .bus import MessageBus, full_mesh_policy, star_policy
from .facts import Fact
from .runtime import Runtime

# ---- topic 词表 ---------------------------------------------------------
T_SCOPE = "scope"
T_SLOWQ = "slow_query"
T_INDEX = "index_state"
T_RELEASE = "release_meta"
T_RATE = "rate_limit"
T_QUERYCHG = "query_change"
T_ROOT = "root_cause"
T_REPORT = "final_report"

# 一个 topic 可以有多个知情者 —— 这是本场景最重要的结构差异
DIRECTORY: dict[str, list[str]] = {
    T_SCOPE: ["gateway"],
    T_SLOWQ: ["dba"],
    T_INDEX: ["dba", "audit"],       # 过期快照 vs 权威记录
    T_RELEASE: ["deploy", "audit"],  # 表面冲突
    T_RATE: ["gateway", "audit"],    # 传闻 vs 权威否定
    T_QUERYCHG: ["ordersvc"],
    T_ROOT: ["sre"],
    T_REPORT: ["sre"],
}

# 时间刻度：
#   NOW_FACT = None  表示「实时」—— 在被说出口的那一刻才落地成具体刻度；
#   STALE    = -100  表示一份 100 个时间单位前的历史快照。
NOW_FACT = None
STALE = -100
FRESH_WITHIN = 25   # 只有历史快照会超过这个阈值，本轮产生的信息不会

ROSTER = ["sre", "gateway", "dba", "deploy", "audit", "ordersvc"]
TITLES = {
    "sre": "值班 SRE / 结案",
    "gateway": "API 网关",
    "dba": "数据库团队",
    "deploy": "发布平台",
    "audit": "变更审计",
    "ordersvc": "订单服务",
}

TOKENS = ["status IN", "回滚", "排除", "SeqScan"]


def _knowledge() -> dict[str, dict[str, Fact]]:
    return {
        "gateway": {
            T_SCOPE: Fact(
                "网关自身 P99 仅 8ms，外部 QPS 稳定在 1200/s；瓶颈在下游数据库连接等待（320ms）。"
                "另外提醒：昨晚 23:00 上线过一批限流规则，不排除与本次告警有关。",
                confidence=0.90, as_of=NOW_FACT, evidence="网关自身指标",
            ),
            # 干扰项：低置信度的传闻。核实成本很低，但不核实就会被带偏。
            T_RATE: Fact(
                "昨晚 23:00 上线了一批新的限流规则，可能与本轮告警有关。",
                confidence=0.60, as_of=NOW_FACT, evidence="值班群里的口头同步",
            ),
        },
        "dba": {
            T_SLOWQ: Fact(
                "order_items 出现全表扫描，单次耗时 780ms（基线为索引扫描 12ms）。",
                confidence=0.90, as_of=NOW_FACT, evidence="慢查询日志 Top1",
            ),
            # 过期陷阱：这句话没错，它只是旧了。
            # 置信度**故意设得很高**（0.92）—— 这是本场景最难识别的一类脏数据。
            # 只校验置信度的 Agent 会照单全收；只有同时校验新鲜度才拦得住它。
            T_INDEX: Fact(
                "idx_order_items_status 在 order_items 上存在。",
                confidence=0.92, as_of=STALE, evidence="Schema 快照（100 个时间单位前，未刷新）",
            ),
        },
        "deploy": {
            # 表面冲突的一半：他说的没错，但他不知道清单里那条已回滚的 DDL。
            T_RELEASE: Fact(
                "v2.31 于昨日 14:00 发布，本次发布不含任何 DDL 或索引变更。",
                confidence=0.85, as_of=NOW_FACT, evidence="发布单（仅列表页）",
            ),
        },
        "audit": {
            T_INDEX: Fact(
                "v2.31 变更清单中包含 DROP INDEX idx_order_items_status，执行状态=已回滚；"
                "索引当前仍然存在。",
                confidence=0.95, as_of=NOW_FACT, evidence="变更审计流水（逐条执行记录）",
            ),
            T_RELEASE: Fact(
                "v2.31 变更清单共 7 项，其中 1 项 DDL（DROP INDEX）已回滚，实际生效的只有应用层代码变更。",
                confidence=0.95, as_of=NOW_FACT, evidence="变更审计流水",
            ),
            # 干扰项的解药
            T_RATE: Fact(
                "限流规则变更已于 23:40 回滚，与本次告警起始时间（14:00）不吻合，应排除。",
                confidence=0.95, as_of=NOW_FACT, evidence="变更审计流水",
            ),
        },
        "ordersvc": {
            T_QUERYCHG: Fact(
                "v2.31 将下单查询的 WHERE 谓词由 status='NEW' 重写为 status IN (6 个值)，"
                "原 idx_order_items_status 单列索引不再匹配新谓词。",
                confidence=0.95, as_of=NOW_FACT, evidence="代码 diff",
            ),
        },
        "sre": {},
    }


REPORT_TEMPLATE = (
    "【事故根因报告】症状：{slow_query}"
    "｜发布侧核查：{release_meta}"
    "｜索引状态：{index_state}"
    "｜触发变更：{query_change}"
    "｜干扰项核查：{rate_limit}"
    "｜结论：查询谓词与索引失配导致 SeqScan，连接池被打满。"
)


def build(
    disposition: str = SKEPTICAL,
    topology: str = "peer",
    hub: str = "sre",
    ask_budget: int = 8,
    max_rounds: int = 60,
) -> Runtime:
    """组装脏环境。

    disposition: "gullible" 每个 topic 只问第一个知情者 / "skeptical" 问遍所有知情者
    topology:    "peer" 对等全连通 / "star" 一切经 sre 转述
    """
    knowledge = _knowledge()
    common = dict(
        disposition=disposition,
        ask_budget=ask_budget,
        fresh_within=FRESH_WITHIN,
    )

    brains: dict[str, DirtyBrain] = {
        "sre": DirtyBrain(
            knowledge=knowledge["sre"],
            triggers=((T_SCOPE, T_SLOWQ), (T_SLOWQ, T_INDEX), (T_SLOWQ, T_QUERYCHG), (T_SCOPE, T_RATE)),
            requirement=(
                {T_SLOWQ, T_INDEX, T_RELEASE, T_RATE, T_QUERYCHG},
                T_REPORT,
                REPORT_TEMPLATE,
            ),
            **common,
        ),
        "gateway": DirtyBrain(knowledge=knowledge["gateway"], notify="sre", **common),
        "dba": DirtyBrain(
            knowledge=knowledge["dba"],
            triggers=((T_SLOWQ, T_RELEASE),),
            notify="sre",
            **common,
        ),
        "deploy": DirtyBrain(knowledge=knowledge["deploy"], notify="sre", **common),
        "audit": DirtyBrain(knowledge=knowledge["audit"], notify="sre", **common),
        "ordersvc": DirtyBrain(
            knowledge=knowledge["ordersvc"],
            triggers=((T_QUERYCHG, T_INDEX),),
            notify="sre",
            unresponsive_times=2,   # 前两次提问不回 —— 逼出重试机制
            **common,
        ),
    }

    if topology == "peer":
        bus = MessageBus(policy=full_mesh_policy)
        active_hub = None
    else:
        bus = MessageBus(policy=star_policy(hub), hub=hub)
        active_hub = hub

    board = Blackboard()
    board.publish_directory(DIRECTORY)

    agents = [Agent(aid, TITLES[aid], brains[aid], bus) for aid in ROSTER]
    bus.register(*[a.id for a in agents])
    return Runtime(agents, bus, board, hub=active_hub, max_rounds=max_rounds)


def kickoff(runtime: Runtime) -> None:
    """SRE 接到告警，把第一问发给网关（症状最先在那里露出）。"""
    from .message import TASK, Message

    runtime.bus.send(
        Message(
            sender="sre",
            recipients=("gateway",),
            kind=TASK,
            topic=T_SCOPE,
            content="checkout 接口 P99 由 120ms 劣化到 900ms，请确认是否与网关侧有关。",
        )
    )
