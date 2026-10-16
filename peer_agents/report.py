"""把一次运行渲染成人能读的东西：控制台 trace + HTML 对比报告。"""

from __future__ import annotations

import html
import json
from pathlib import Path

from .runtime import RunResult
from .topology import topology_svg

MODE_TAG = {
    "对等协作（全连通）": ("#0F6E56", "#E1F5EE"),
    "星型转述（中心化）": ("#185FA5", "#E6F1FB"),
    "星型转述 + 主 Agent 摘要": ("#A32D2D", "#FCEBEB"),
    "自适应拓扑（预算3）": ("#534AB7", "#EEEDFE"),
    "自适应拓扑（预算2）": ("#993556", "#FBEAF0"),
}

NODES = ["sre", "gateway", "dba", "deploy", "ordersvc"]


def transcript(result: RunResult, hub: str | None = None) -> str:
    lines = [f"  ── {result.name} ──"]
    for i, m in enumerate(result.messages, 1):
        if m.relay_to:
            arrow, flag = "⇢", " [枢纽代投]"
        elif len(m.path) > 2:
            arrow, flag = "⇒", f" [经 {'→'.join(m.path[1:-1])} 多跳转发]"
        else:
            arrow, flag = "→", ""
        lines.append(f"  {i:>3}. {m.sender:>9} {arrow} {m.targets:<12}{flag} {m.kind}:{m.topic}")
    return "\n".join(lines)


def summary_table(results: list[RunResult], hub: str) -> str:
    head = (
        f"{'配置':<26}{'业务消息':>9}{'控制消息':>9}{'轮次':>6}{'横向':>6}"
        f"{'枢纽负载':>9}{'中转':>6}{'多跳':>6}{'保真度':>8}"
    )
    rows = [head, "-" * len(head.encode("gbk", errors="ignore"))]
    rows.extend(
        f"{r.name:<26}{r.data_messages:>9}{r.control_messages:>9}{r.rounds:>6}"
        f"{r.lateral(hub):>6}{r.hub_load(hub):>9}{r.relay_touches:>6}"
        f"{r.transit_hops:>6}{r.fidelity * 100:>7.0f}%"
        for r in results
    )
    return "\n".join(rows)


def write_html(results: list[RunResult], hub: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cards = []
    for r in results:
        color, _bg = MODE_TAG.get(r.name, ("#444441", "#F1EFE8"))
        steps = []
        for m in r.messages:
            if m.relay_to:
                badge, cls = "代投", "k relay"
            elif len(m.path) > 2:
                badge, cls = f"{len(m.path) - 1}跳", "k route"
            else:
                badge, cls = m.kind, "k"
            note = ""
            if m.relay_to:
                note = " hub"
            elif len(m.path) > 2:
                note = f' <span class="mid">{"→".join(m.path[1:-1])}</span>'
            steps.append(
                f'<li><span class="{cls}">{html.escape(badge)}</span>'
                f'<span class="who">{html.escape(m.sender)}</span>'
                f'<span class="arw">&rarr;</span>'
                f'<span class="who">{html.escape(m.targets)}</span>'
                f'<span class="tp">{html.escape(m.topic)}</span>{note}</li>'
            )

        topo_html = ""
        if r.adaptive:
            init = topology_svg(
                NODES, r.initial_edges or [], None, "初始拓扑",
                f"{len(r.initial_edges or [])} 条链路 · 数据库组是孤岛",
            )
            final = topology_svg(
                NODES, r.final_edges or [], r.edge_traffic or {}, "最终拓扑",
                f"{len(r.final_edges or [])} 条链路 · 边上的数字是承载消息数",
            )
            topo_html = (
                f'<div class="topo">{init}{final}</div>'
                f'<p class="toponote">'
                f'建链协商 {r.link_requests} 次（接受 {r.link_accepts} / 拒绝 {r.link_rejects}），'
                f'因链路预算满而淘汰旧连接 <b>{r.evictions}</b> 次，'
                f'多跳转发 {r.transit_hops} 次，平均路径长度 {r.avg_path_len:.2f} 跳。'
                f'</p>'
            )

        cards.append(
            f'<section class="card">'
            f'<h2 style="color:{color}">{html.escape(r.name)}</h2>'
            f'<div class="metrics">'
            f'<div><b>{r.data_messages}</b><span>业务消息</span></div>'
            f'<div><b>{r.control_messages}</b><span>控制面协商</span></div>'
            f'<div><b>{r.rounds}</b><span>协作轮次</span></div>'
            f'<div><b>{r.lateral(hub)}</b><span>横向消息</span></div>'
            f'<div><b>{r.hub_load(hub)}</b><span>枢纽负载</span></div>'
            f'<div><b>{r.fidelity * 100:.0f}%</b><span>信息保真度</span></div>'
            f'</div>'
            f'{topo_html}'
            f'<p class="report" style="border-color:{color}">{html.escape(r.final_report)}</p>'
            f'<details><summary>完整消息序列（{r.total_messages} 条）</summary>'
            f'<ol class="trace">{"".join(steps)}</ol></details>'
            f'</section>'
        )

    peer, star = results[0], results[1]
    delta = (star.data_messages / peer.data_messages - 1) * 100 if peer.data_messages else 0
    ad = [r for r in results if r.adaptive]
    adapt_line = ""
    if ad:
        a3 = ad[0]
        adapt_line = (
            f'<br><br>后两组更进一步：<b>连拓扑本身都是 Agent 自己谈出来的</b>。'
            f'开局只有 3 条链路、数据库组甚至是孤岛。第一组自适应实验里，'
            f'Agent 用 {a3.control_messages} 条建链协商报文把网络扩成 {len(a3.final_edges or [])} 条链路，'
            f'业务消息量最终回到 {a3.data_messages} 条 —— 与「上帝视角预先全连通」的对等架构完全持平，'
            f'而没有任何节点需要承担全局上下文。'
        )
        if len(ad) > 1:
            a2 = ad[1]
            adapt_line += (
                f'<br>把链路预算从 3 收紧到 2 后出现了反直觉的结果：'
                f'最终只剩 {len(a2.final_edges or [])} 条链路（更精简），'
                f'却触发了 <b>{a2.evictions} 次 LRU 淘汰</b>，平均路径从 '
                f'{a3.avg_path_len:.2f} 跳缩短到 <b>{a2.avg_path_len:.2f} 跳</b>，'
                f'业务消息量与保真度纹丝不动。'
                f'原因很简单：预算紧张时，被淘汰的都是低频连接，留下来的是真正高频的骨干 —— '
                f'拓扑在自我优化。'
            )

    doc = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>多 Agent 通信拓扑对比实验</title>
<style>
  :root {{ color-scheme: light; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; background: #FBFAF7; color: #2C2C2A;
          font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif; }}
  .wrap {{ max-width: 900px; margin: 0 auto; padding: 36px 20px 60px; }}
  h1 {{ font-size: 20px; font-weight: 500; margin: 0 0 8px; }}
  .sub {{ font-size: 13px; color:#5F5E5A; margin: 0 0 18px; line-height: 1.7; }}
  .lead {{ font-size: 13px; line-height: 1.85; background:#F1EFE8; border-radius: 10px;
           padding: 14px 16px; margin: 0 0 22px; }}
  .lead b {{ font-weight: 500; }}
  .cards {{ display: flex; flex-direction: column; gap: 14px; }}
  .card {{ border:1px solid rgba(0,0,0,.12); border-radius: 12px; padding: 16px 18px; background:#fff; }}
  .card h2 {{ font-size: 14px; font-weight: 500; margin: 0 0 12px; }}
  .metrics {{ display:flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }}
  .metrics div {{ flex:1 1 105px; background:#F7F6F2; border-radius: 8px; padding: 8px 10px; }}
  .metrics b {{ display:block; font-size: 18px; font-weight:500; }}
  .metrics span {{ font-size: 11px; color:#5F5E5A; }}
  .report {{ font-size: 13px; line-height:1.75; margin: 0 0 12px; padding: 10px 12px;
             border-left: 3px solid #888780; background:#FAFAF7; border-radius: 4px; }}
  details summary {{ font-size: 12px; color:#5F5E5A; cursor: pointer; }}
  ol.trace {{ list-style:none; margin: 10px 0 0; padding: 0; font-size: 12px; line-height: 1.95; }}
  ol.trace li {{ display:flex; gap: 8px; align-items: baseline; border-bottom:1px solid rgba(0,0,0,.05); }}
  .k {{ flex: 0 0 56px; font-weight: 500; color:#185FA5; }}
  .k.relay {{ color:#A32D2D; }}
  .k.route {{ color:#534AB7; }}
  .who {{ flex: 0 0 auto; }}
  .arw {{ color:#888780; }}
  .mid {{ color:#534AB7; font-size:11px; }}
  .tp {{ margin-left:auto; color:#888780; font-family: ui-monospace, Consolas, monospace; font-size:11px; }}
  .topo {{ display:flex; gap: 14px; margin: 4px 0 6px; }}
  .topo > svg {{ flex: 1 1 0; min-width: 0; background:#FAFAF7; border-radius: 10px; padding: 4px; }}
  .toponote {{ font-size: 12px; color:#5F5E5A; line-height:1.8; margin: 0 0 14px; }}
  .toponote b {{ font-weight: 500; color:#534AB7; }}
</style>
</head>
<body>
<div class="wrap">
<h1>多 Agent 通信拓扑对比实验</h1>
<p class="sub">同一场生产故障定位任务，只改变一个变量：子 Agent 之间能不能直接说话。</p>
<p class="lead"><b>结论：</b>放开横向通信后，业务消息量从 {star.data_messages} 条降到 {peer.data_messages} 条（−{delta:.0f}%），
协作轮次从 {star.rounds} 轮降到 {peer.rounds} 轮，主 Agent 的上下文负载从 {star.hub_load(hub)} 条降到 {peer.hub_load(hub)} 条。
更关键的是信息保真度：中心化架构下主 Agent 一旦「转述」而不是「转发」，关键细节就会当场丢失 —— 见第三组，终报保真度掉到 50%。{adapt_line}</p>
<div class="cards">{"".join(cards)}</div>
</div>
</body>
</html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(doc, encoding="utf-8")
    return path


def write_json(results: list[RunResult], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = [
        {
            "name": r.name,
            "rounds": r.rounds,
            "total_messages": r.total_messages,
            "fidelity": r.fidelity,
            "final_report": r.final_report,
            "final_edges": [list(e) for e in (r.final_edges or [])],
            "edge_traffic": {f"{a}|{b}": v for (a, b), v in (r.edge_traffic or {}).items()},
            "link_requests": r.link_requests,
            "link_accepts": r.link_accepts,
            "link_rejects": r.link_rejects,
            "evictions": r.evictions,
            "transit_hops": r.transit_hops,
            "messages": [
                {"i": i, "sender": m.sender, "to": list(m.recipients), "kind": m.kind,
                 "topic": m.topic, "relay": bool(m.relay_to), "path": list(m.path),
                 "content": m.content}
                for i, m in enumerate(r.messages, 1)
            ],
        }
        for r in results
    ]
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
