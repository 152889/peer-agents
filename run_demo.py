"""演示入口：同一场故障，三种通信拓扑，一次跑完。

    python run_demo.py

它会：
  1. 分别以「对等协作 / 星型转述 / 星型转述+摘要」三种配置跑同一个场景；
  2. 打印完整消息序列，让你看清每一次「谁跟谁说了什么」；
  3. 统计消息数、轮次、横向消息、枢纽负载、信息保真度；
  4. 在 out/ 下生成 comparison.html 与三份原始 trace(JSON)。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")  # 保证中文在 Windows 控制台正常输出

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from peer_agents import message as M  # noqa: E402
from peer_agents.report import summary_table, transcript, write_html, write_json  # noqa: E402
from peer_agents.scenario_incident import (  # noqa: E402
    GROUND_TRUTH_TOKENS,
    T_SCOPE,
    build,
)

HUB = "sre"
CONFIGS = [
    ("对等协作（全连通）", dict(topology="peer", hub_summarize_chars=None)),
    ("星型转述（中心化）", dict(topology="star", hub_summarize_chars=None)),
    ("星型转述 + 主 Agent 摘要", dict(topology="star", hub_summarize_chars=38)),
    ("自适应拓扑（预算3）", dict(topology="adaptive", link_budget=3)),
    ("自适应拓扑（预算2）", dict(topology="adaptive", link_budget=2)),
]


def bootstrap(runtime) -> None:
    """SRE 发起任务：症状最先在网关暴露，所以把首个 TASK 发给网关。"""
    runtime.bus.send(
        M.Message(
            sender=HUB,
            recipients=("gateway",),
            kind=M.TASK,
            topic=T_SCOPE,
            content="checkout 接口 P99 由 120ms 劣化到 900ms，请定位你这一侧是否有异常。",
        )
    )


def main() -> None:
    results = []
    print("=" * 78)
    print("多 Agent 通信拓扑对比实验 · 场景：checkout P99 劣化根因定位")
    print("=" * 78)

    for name, cfg in CONFIGS:
        runtime, board = build(**cfg)
        bootstrap(runtime)
        r = runtime.run(name=name, ground_truth_tokens=GROUND_TRUTH_TOKENS)
        results.append(r)

        print()
        print(f"▶ {name}")
        print(transcript(r, HUB))
        print(f"  终报：{r.final_report[:110]}{'…' if len(r.final_report) > 110 else ''}")
        if r.adaptive:
            def _edges(es):
                return " ".join(f"{a}—{b}" for a, b in (es or [])) or "（无）"

            print(f"  拓扑演化：{len(r.initial_edges or [])} 条 → {len(r.final_edges or [])} 条")
            print(f"        初始：{_edges(r.initial_edges)}")
            print(f"        最终：{_edges(r.final_edges)}")
            print(f"        建链 {r.link_requests} 次（接受 {r.link_accepts} / 拒绝 {r.link_rejects}）"
                  f"· 淘汰 {r.evictions} · 多跳转发 {r.transit_hops} 次 · 平均路径 {r.avg_path_len:.2f} 跳")
        if r.dropped:
            print(f"  被丢弃：{len(r.dropped)} 条")

    print()
    print("=" * 78)
    print("指标对比")
    print("=" * 78)
    print(summary_table(results, HUB))
    print()
    print("业务消息 = 承载协作内容的报文；控制消息 = 拓扑协商报文（一次性投资，单独计）")
    print("横向     = 子 Agent 之间不经枢纽的直接通信条数")
    print("枢纽负载 = 枢纽经手的业务消息条数，可直接换算成主 Agent 的上下文开销")
    print("中转     = 靠枢纽代投转述的条数；多跳 = 靠中间节点纯转发穿行的跳数")
    print("保真度   = 终报中仍保留的关键信息 token 占比")

    html_path = write_html(results, HUB, ROOT / "out" / "comparison.html")
    json_path = write_json(results, ROOT / "out" / "trace.json")
    print()
    print(f"HTML 报告：{html_path}")
    print(f"原始 trace：{json_path}")


if __name__ == "__main__":
    main()
