"""把脏环境的四组实验渲染成一份可读的 HTML 观测报告。

与 report.py 的区别：那份报告关心「消息怎么走」，这份关心「谁信了什么」。
所以主体不是消息序列，而是每个 Agent 的**认知账本** ——
他最后采信了谁的哪一版、当场拒了谁、在哪里改了口。
"""

from __future__ import annotations

import html
import json
from pathlib import Path

TAG = {
    ("skeptical", "peer"): ("#0F6E56", "#E1F5EE", "怀疑 · 对等"),
    ("skeptical", "star"): ("#185FA5", "#E6F1FB", "怀疑 · 星型"),
    ("gullible", "peer"): ("#993556", "#FBEAF0", "轻信 · 对等"),
    ("gullible", "star"): ("#A32D2D", "#FCEBEB", "轻信 · 星型"),
}

# 三个「有人给了不同答案」的话题 —— 看每个 Agent 最后站了谁
CONTESTED = [
    ("index_state", "索引还在不在", "dba（过期快照）", "audit（变更流水）"),
    ("rate_limit", "限流变更是原因吗", "gateway（低置信传闻）", "audit（权威否定）"),
    ("release_meta", "本次发布含 DDL 吗", "deploy（发布单）", "audit（逐条执行记录）"),
]


def _esc(s: str) -> str:
    return html.escape(s or "")


def _metrics(r, hub: str) -> str:
    cells = [
        (f"{r.rounds}", "协作轮次"),
        (f"{r.data_messages}", "业务消息"),
        (f"{r.hub_load(hub)}", "枢纽负载"),
        (f"{r.lateral(hub)}", "横向消息"),
        (f"{r.fidelity * 100:.0f}%", "终报保真度"),
    ]
    return "".join(f"<div><b>{v}</b><span>{k}</span></div>" for v, k in cells)


def _verdicts(sre) -> str:
    """枢纽对三个争议话题的最终裁决。"""
    rows = []
    for topic, label, bad, good in CONTESTED:
        f = sre.ledger.get(topic)
        oppose = f'<em>对立版本：{_esc(bad)} vs {_esc(good)}</em>'
        if f is None:
            rows.append(f'<li><span class="q">{_esc(label)}{oppose}</span>'
                        f'<span class="none">未裁决</span></li>')
            continue
        # 三个话题的「正确答案」都在审计手上：它给的是权威、新鲜、交叉过的版本
        ok = f.source == "audit"
        cls, mark = ("ok", "✓") if ok else ("bad", "✗")
        rows.append(
            f'<li><span class="q">{_esc(label)}{oppose}</span>'
            f'<span class="{cls}">{mark} 站了 {_esc(f.source or "?")}</span>'
            f'<span class="meta">conf={f.confidence:.2f}'
            f'{" · 快照" if f.as_of is not None and f.as_of < 0 else ""}</span></li>'
        )
    return f'<ul class="verdicts">{"".join(rows)}</ul>'


def _ledger(rt) -> str:
    rows = []
    for a in rt.agents:
        b = a.brain
        known = "、".join(sorted(b.ledger)) or "—"
        rejected = "".join(
            f'<li>「{_esc(t)}」← {_esc(f.source or "?")}'
            f'<span class="meta">conf={f.confidence:.2f}'
            f'{"" if f.as_of is None else f" · as_of={f.as_of}"}</span></li>'
            for t in sorted(b.rejected) for f in b.rejected[t]
        )
        bad = "".join(f'<li>「{_esc(t)}」</li>' for t in sorted(b.conflicts))
        silent = (f'<li>对前 {b.silent_count} 次提问保持沉默</li>') if b.silent_count else ""
        rows.append(
            f'<tr><td class="who">{_esc(a.id)}</td>'
            f'<td>{_esc(known)}</td>'
            f'<td><ul class="mini">{rejected}</ul></td>'
            f'<td><ul class="mini">{bad}{silent}</ul></td>'
            f'<td class="num">{b.overrides}</td>'
            f'<td class="num">{b.budget_used}/{b.ask_budget}</td></tr>'
        )
    return (
        '<table class="ledger"><thead><tr>'
        '<th>Agent</th><th>最终采信</th><th>当场拒收</th><th>发现分歧</th>'
        '<th>自我更正</th><th>提问预算</th>'
        f'</tr></thead><tbody>{"".join(rows)}</tbody></table>'
    )


def _trace(r) -> str:
    seen = set()
    items = []
    for m in r.messages:
        if m.msg_id in seen:
            continue
        seen.add(m.msg_id)
        badge, cls = m.kind, "k"
        if m.relay_to:
            badge, cls = "代投", "k relay"
        note = f' <span class="mid">第{m.attempt + 1}次</span>' if m.attempt else ""
        to = "全体" if "*" in m.recipients else "/".join(m.recipients)
        items.append(
            f'<li><span class="{cls}">{_esc(badge)}</span>'
            f'<span class="who">{_esc(m.sender)}</span>'
            f'<span class="arw">&rarr;</span>'
            f'<span class="who">{_esc(to)}</span>'
            f'<span class="tp">{_esc(m.topic)}</span>{note}</li>'
        )
    return f'<ol class="trace">{"".join(items)}</ol>'


def write_dirty_html(runs, hub: str, path: Path) -> Path:
    """runs: [(disposition, topology, runtime, result), ...]"""
    by = {(d, t): (rt, r) for d, t, rt, r in runs}
    sk_p = by[("skeptical", "peer")][1]
    sk_s = by[("skeptical", "star")][1]
    gu_p = by[("gullible", "peer")][1]
    gu_s = by[("gullible", "star")][1]

    cards = []
    for disp, topo, rt, r in runs:
        color, bg, label = TAG[(disp, topo)]
        sre = next(a.brain for a in rt.agents if a.id == hub)
        cards.append(
            f'<section class="card" style="--accent:{color};--tint:{bg}">'
            f'<h2><span class="tag">{_esc(label)}</span>'
            f'<span class="fid" style="color:{color}">保真度 {r.fidelity * 100:.0f}%</span></h2>'
            f'<div class="metrics">{_metrics(r, hub)}</div>'
            f'<h3>枢纽对三个争议话题的裁决</h3>{_verdicts(sre)}'
            f'<p class="report">{_esc(r.final_report)}</p>'
            f'<details><summary>认知账本（每个 Agent 信了什么、拒了什么）</summary>'
            f'{_ledger(rt)}</details>'
            f'<details><summary>完整消息序列（{r.data_messages} 条业务消息）</summary>'
            f'{_trace(r)}</details>'
            f'</section>'
        )

    delta = (sk_s.data_messages / sk_p.data_messages - 1) * 100

    doc = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>极度真实环境 · 子 Agent 交流观测</title>
<style>
  :root {{ color-scheme: light; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:#FBFAF7; color:#2C2C2A;
          font-family:-apple-system,"Segoe UI","Microsoft YaHei",sans-serif; }}
  .wrap {{ max-width:960px; margin:0 auto; padding:36px 20px 64px; }}
  h1 {{ font-size:21px; font-weight:500; margin:0 0 8px; }}
  .sub {{ font-size:13px; color:#5F5E5A; margin:0 0 18px; line-height:1.75; }}
  .lead {{ font-size:13px; line-height:1.9; background:#F1EFE8; border-radius:10px;
           padding:15px 17px; margin:0 0 20px; }}
  .lead b {{ font-weight:500; color:#0F6E56; }}
  .dirt {{ display:flex; flex-wrap:wrap; gap:8px; margin:0 0 24px; }}
  .dirt span {{ font-size:12px; background:#fff; border:1px solid rgba(0,0,0,.1);
                border-radius:999px; padding:5px 11px; }}
  .cards {{ display:flex; flex-direction:column; gap:16px; }}
  .card {{ border:1px solid rgba(0,0,0,.12); border-left:3px solid var(--accent);
           border-radius:12px; padding:16px 18px; background:#fff; }}
  .card h2 {{ font-size:14px; font-weight:500; margin:0 0 12px;
              display:flex; align-items:center; justify-content:space-between; }}
  .tag {{ background:var(--tint); color:var(--accent); border-radius:6px; padding:3px 9px; }}
  .fid {{ font-size:13px; }}
  .metrics {{ display:flex; gap:8px; flex-wrap:wrap; margin-bottom:14px; }}
  .metrics div {{ flex:1 1 92px; background:#F7F6F2; border-radius:8px; padding:8px 10px; }}
  .metrics b {{ display:block; font-size:17px; font-weight:500; }}
  .metrics span {{ font-size:11px; color:#5F5E5A; }}
  h3 {{ font-size:12px; font-weight:500; color:#5F5E5A; margin:0 0 8px; }}
  ul.verdicts {{ list-style:none; margin:0 0 14px; padding:0; font-size:12.5px; }}
  ul.verdicts li {{ display:flex; gap:10px; align-items:baseline; flex-wrap:wrap;
                    padding:6px 0; border-bottom:1px solid rgba(0,0,0,.05); }}
  ul.verdicts .q {{ flex:0 0 210px; }}
  ul.verdicts .q em {{ display:block; font-style:normal; font-size:11px; color:#888780; }}
  .ok {{ color:#0F6E56; font-weight:500; }}
  .bad {{ color:#A32D2D; font-weight:500; }}
  .none {{ color:#888780; }}
  .meta {{ font-size:11px; color:#888780; font-family:ui-monospace,Consolas,monospace; }}
  .report {{ font-size:12.5px; line-height:1.8; margin:0 0 14px; padding:11px 13px;
             border-left:3px solid #CFCBC0; background:#FAFAF7; border-radius:4px; color:#444441; }}
  details summary {{ font-size:12px; color:#5F5E5A; cursor:pointer; padding:5px 0; }}
  table.ledger {{ width:100%; border-collapse:collapse; font-size:12px; margin:8px 0 4px; }}
  table.ledger th {{ text-align:left; font-weight:500; color:#5F5E5A;
                     border-bottom:1px solid rgba(0,0,0,.12); padding:6px 8px; font-size:11px; }}
  table.ledger td {{ vertical-align:top; border-bottom:1px solid rgba(0,0,0,.05); padding:7px 8px; }}
  table.ledger td.who {{ font-weight:500; white-space:nowrap; }}
  table.ledger td.num {{ text-align:center; white-space:nowrap; }}
  ul.mini {{ list-style:none; margin:0; padding:0; }}
  ul.mini li {{ font-size:11.5px; line-height:1.6; }}
  ol.trace {{ list-style:none; margin:8px 0 0; padding:0; font-size:12px; line-height:1.9; }}
  ol.trace li {{ display:flex; gap:8px; align-items:baseline;
                 border-bottom:1px solid rgba(0,0,0,.05); }}
  .k {{ flex:0 0 56px; font-weight:500; color:#185FA5; }}
  .k.relay {{ color:#A32D2D; }}
  .arw {{ color:#888780; }}
  .mid {{ font-size:11px; color:#993556; }}
  .tp {{ margin-left:auto; color:#888780; font-size:11px;
         font-family:ui-monospace,Consolas,monospace; }}
  .findings {{ margin-top:26px; font-size:13px; line-height:1.9; }}
  .findings h2 {{ font-size:15px; font-weight:500; margin:0 0 10px; }}
  .findings ol {{ padding-left:20px; }}
  .findings li {{ margin-bottom:11px; }}
  .findings b {{ font-weight:500; }}
</style>
</head>
<body>
<div class="wrap">
<h1>极度真实环境 · 观察子 Agent 之间的交流</h1>
<p class="sub">同一场生产故障定位。这次环境里有人给旧数据、有人给传闻、
有人给一半的真相、有人在救火不回消息。<br>
唯一被刻意设计的目标：让「对等一定更好」这句话站不住。</p>

<div class="dirt">
  <span>一个 topic 多个知情者</span>
  <span>高置信度过期快照</span>
  <span>表面冲突</span>
  <span>低置信度干扰项</span>
  <span>不可靠成员</span>
</div>

<p class="lead"><b>结论先说：保真度由认知纪律决定，与拓扑无关。</b><br>
无论消息走不走中心，「先到的就信」都会照单收下过期快照和低置信度传闻 ——
轻信策略在对等与星型下都是 {gu_p.fidelity * 100:.0f}%；
而怀疑策略在两种拓扑下都是 {sk_p.fidelity * 100:.0f}%。
拓扑只决定代价：星型比对等多付 {delta:.0f}% 的通信量，且这些开销全部压在枢纽身上
（枢纽负载 {sk_s.hub_load(hub)} 对 {sk_p.hub_load(hub)}）。对等架构的价值不在「更对」，而在「更省」。</p>

<div class="cards">{"".join(cards)}</div>

<div class="findings">
<h2>观察到的三件事</h2>
<ol>
<li><b>保真度由「认知风格」决定，与拓扑无关。</b>
   怀疑：对等 {sk_p.fidelity * 100:.0f}% / 星型 {sk_s.fidelity * 100:.0f}%；
   轻信：对等 {gu_p.fidelity * 100:.0f}% / 星型 {gu_s.fidelity * 100:.0f}%。
   换成对等拓扑，救不了不设防的认知。</li>
<li><b>拓扑只决定「代价」，不决定「对错」。</b>
   对等 {sk_p.data_messages} 条业务消息、枢纽负载 {sk_p.hub_load(hub)}；
   星型 {sk_s.data_messages} 条、枢纽负载 {sk_s.hub_load(hub)}。
   对等架构里有 {sk_p.lateral(hub)} 条消息压根没经过枢纽 ——
   它的上下文里不必装下整场对话。</li>
<li><b>「怀疑」不是免费的。</b>
   它多看、多问、多比对：业务消息 {sk_p.data_messages} 条（轻信只有 {gu_p.data_messages} 条），
   并触发了 {next(a.brain for a in by[("skeptical", "peer")][0].agents if a.id == hub).overrides}
   次自我更正 —— 先采信了发布平台的「无 DDL」，拿到审计的逐条执行记录后才改口。
   而轻信策略里这个数字是 0：它连改口的机会都没有。</li>
</ol>
</div>
</div>
</body>
</html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(doc, encoding="utf-8")
    return path


def write_dirty_json(runs, hub: str, path: Path) -> Path:
    """把每组的认知账本落成 JSON，便于做量化分析或画图。

    JSON 里刻意保留了「谁在什么时候信了谁」的原始记录 ——
    报告是给人的，JSON 是给下一次实验的。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = []
    for disp, topo, rt, r in runs:
        sre = next(a.brain for a in rt.agents if a.id == hub)
        now = rt.board.round
        data.append({
            "disposition": disp,
            "topology": topo,
            "rounds": r.rounds,
            "data_messages": r.data_messages,
            "hub_load": r.hub_load(hub),
            "lateral": r.lateral(hub),
            "fidelity": r.fidelity,
            "final_report": r.final_report,
            "hub_verdicts": {
                t: ({"source": f.source, "confidence": f.confidence, "as_of": f.as_of}
                    if (f := sre.ledger.get(t)) else None)
                for t, *_ in CONTESTED
            },
            "agents": [
                {
                    "id": a.id,
                    "ledger": {t: {"value": f.value, "confidence": f.confidence,
                                   "as_of": f.as_of, "source": f.source}
                               for t, f in a.brain.ledger.items()},
                    "rejected": {t: [{"value": f.value, "confidence": f.confidence,
                                      "as_of": f.as_of, "source": f.source}
                                     for f in fs]
                                 for t, fs in a.brain.rejected.items()},
                    "conflicts": sorted(a.brain.conflicts),
                    "overrides": a.brain.overrides,
                    "budget_used": a.brain.budget_used,
                    "ask_budget": a.brain.ask_budget,
                    "stale_accepts": a.brain.stale_accepts(now),
                    "silent_count": a.brain.silent_count,
                }
                for a in rt.agents
            ],
            "messages": [
                {"sender": m.sender, "to": list(m.recipients), "kind": m.kind,
                 "topic": m.topic, "attempt": m.attempt, "relay": bool(m.relay_to),
                 "msg_id": m.msg_id}
                for m in r.messages
            ],
        })
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
