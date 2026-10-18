"""「极度真实环境」观察入口。

    python run_dirty_demo.py

干净场景里，「问对人」就等于「拿到真相」。真实环境不是这样：
同一个问题有多个知情者，他们说的都对、但都只说了一半；
有人手上是三天前的快照；有人手里是别人的传闻；还有人正在救火，不回消息。

本脚本把 4 种组合跑一遍 —— {轻信，怀疑} × {对等，星型} —— 并打印：

  1. 对话全文：谁跟谁说了什么，谁在什么时候不吭声；
  2. 认知账本：每个 Agent 最后信了什么、当场拒了什么、在哪里改了口；
  3. 对比表：拓扑与认知风格，各自决定什么、代价是什么。

它被刻意设计成**可以对「对等一定更好」这个结论不利**。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from peer_agents import scenario_dirty as S  # noqa: E402
from peer_agents.brain_dirty import GULLIBLE, SKEPTICAL, DirtyBrain  # noqa: E402

HUB = "sre"
CONFIGS = [
    (SKEPTICAL, "peer"),
    (SKEPTICAL, "star"),
    (GULLIBLE, "peer"),
    (GULLIBLE, "star"),
]

DISP_LABEL = {SKEPTICAL: "怀疑", GULLIBLE: "轻信"}
TOPO_LABEL = {"peer": "对等", "star": "星型"}

W = 82


def rule(ch: str = "─") -> str:
    return ch * W


def banner() -> None:
    print(rule("="))
    print("极度真实环境 · 谁在撒谎、谁在过时、谁不回消息")
    print(rule("="))
    print("""
场景里注入了五种脏东西，每一种都针对性地质疑「放开通信就一定更好」：

  1. 一个 topic 多个知情者  「问谁」不再等于「拿到真相」，交叉验证才有意义
  2. 高置信度过期快照      DBA 的 schema 快照 conf=0.92，它没错，只是旧了
  3. 表面冲突              发布平台说「无 DDL」 vs 变更审计说「有一条已回滚的 DDL」
  4. 干扰项                网关抛出一条低置信度传闻「限流变更可能是原因」
  5. 不可靠成员            订单服务正在救火，前两次提问一律不回

真值：v2.31 把谓词由 status='NEW' 改写成 status IN (6 值)，原单列索引失配
      → SeqScan → 连接池打满；而限流变更必须在核实后被**明确排除**。
""")


def show_conversation(res) -> None:
    """按 msg_id 折叠：一次发送（哪怕广播给 5 个人）只显示一行。

    审计日志是按**投递**逐条记录的，一条广播会留下 5 条 —— 那是真实开销，
    但读起来像「发了 5 次」，所以展示层要把它们合回一次发送。
    """
    seen: dict[str, int] = {}
    n = 0
    for m in res.messages:
        if m.msg_id in seen:                    # 同一次发送的后续投递
            continue
        seen[m.msg_id] = 1
        n += 1
        flag = " [代投]" if m.relay_to else ""
        retry = f" (第{m.attempt + 1}次)" if m.attempt else ""
        to = "全体" if "*" in m.recipients else "/".join(m.recipients)
        print(f"  {n:>3}. {m.sender:>9} → {to:<10}{flag}{retry} {m.kind:<7}{m.topic}")


def show_ledger(rt, aid: str) -> None:
    a = next(x for x in rt.agents if x.id == aid)
    b: DirtyBrain = a.brain
    got = ", ".join(sorted(b.ledger)) or "（无）"
    print(f"  [{aid}] 采信了 {len(b.ledger)} 条：{got}")
    for topic in sorted(b.rejected):
        for f in b.rejected[topic]:
            print(f"       ✗ 当场拒收「{topic}」← {f.source or '?'} — "
                  f"conf={f.confidence:.2f} as_of={f.as_of}")
    if b.conflicts:
        print(f"       ⚡ 发现分歧：{', '.join(sorted(b.conflicts))}")
    if b.overrides:
        print(f"       ↻ 自我更正 {b.overrides} 次")
    print(f"       提问预算 {b.budget_used}/{b.ask_budget}")


def run_one(disp: str, topo: str):
    rt = S.build(disposition=disp, topology=topo, hub=HUB)
    S.kickoff(rt)
    res = rt.run(f"{DISP_LABEL[disp]}×{TOPO_LABEL[topo]}",
                 ground_truth_tokens=S.TOKENS)
    return rt, res


def main() -> None:
    banner()
    rows = []
    runs = []

    for disp, topo in CONFIGS:
        rt, res = run_one(disp, topo)
        runs.append((disp, topo, rt, res))
        sre: DirtyBrain = next(x.brain for x in rt.agents if x.id == HUB)
        now = rt.board.round
        team = [x.brain for x in rt.agents]

        print(rule())
        print(f"▶ {DISP_LABEL[disp]} × {TOPO_LABEL[topo]}"
              f"   （{res.rounds} 轮 · {res.data_messages} 条业务消息）")
        print(rule())
        print("对话全文：")
        show_conversation(res)
        print()
        print("认知账本：")
        for aid in ("sre", "dba", "deploy", "audit", "gateway", "ordersvc"):
            show_ledger(rt, aid)
        print()
        hit = [t for t in S.TOKENS if t in res.final_report]
        print(f"终报保真度 {res.fidelity * 100:.0f}%   命中关键信息 {hit}")
        print(f"终报：{res.final_report or '（未产出）'}")
        print()

        rows.append({
            "name": f"{DISP_LABEL[disp]}×{TOPO_LABEL[topo]}",
            "disp": disp, "topo": topo,
            "rounds": res.rounds,
            "data": res.data_messages,
            "hub": res.hub_load(HUB),
            "lateral": res.lateral(HUB),
            "fid": res.fidelity,
            "overrides": sre.overrides,
            "stale": sre.stale_accepts(now),
            "lowconf": sre.low_confidence_accepts(),
            "topics": len(sre.ledger),
            "team_stale": sum(b.stale_accepts(now) for b in team),
        })

    # ---------------------------------------------------------------- 对比表
    print(rule("="))
    print("对比表")
    print(rule("="))
    head = (f"{'配置':<10}{'轮次':>5}{'业务消息':>9}{'枢纽负载':>9}{'横向':>6}"
            f"{'保真度':>8}{'自我更正':>9}{'采信过期':>9}")
    print(head)
    print(rule())
    for r in rows:
        print(f"{r['name']:<10}{r['rounds']:>5}{r['data']:>9}{r['hub']:>9}"
              f"{r['lateral']:>6}{r['fid'] * 100:>7.0f}%{r['overrides']:>9}"
              f"{r['stale']:>9}")

    # ---------------------------------------------------------------- 结论
    by = {(r["disp"], r["topo"]): r for r in rows}
    sk_p, sk_s = by[(SKEPTICAL, "peer")], by[(SKEPTICAL, "star")]
    gu_p, gu_s = by[(GULLIBLE, "peer")], by[(GULLIBLE, "star")]

    print()
    print(rule("="))
    print("观察到的三件事")
    print(rule("="))
    print(f"""
1. 保真度由「认知风格」决定，**与拓扑无关**。
   怀疑：对等 {sk_p['fid'] * 100:.0f}%  星型 {sk_s['fid'] * 100:.0f}%
   轻信：对等 {gu_p['fid'] * 100:.0f}%  星型 {gu_s['fid'] * 100:.0f}%
   无论消息走不走中心，只要「先到的就信」，就会照单收下过期快照和
   低置信度传闻。**换成对等拓扑，救不了不设防的认知。**

2. 拓扑只决定「代价」，不决定「对错」。
   对等 {sk_p['data']} 条业务消息 / 枢纽负载 {sk_p['hub']}
   星型 {sk_s['data']} 条业务消息 / 枢纽负载 {sk_s['hub']}
   星型多付了 {(sk_s['data'] / sk_p['data'] - 1) * 100:.0f}% 的通信量，全部压在枢纽身上；而对等架构里
   有 {sk_p['lateral']} 条消息压根没经过枢纽 —— 它的上下文里不必装下整场对话。

3. 「怀疑」不是免费的。
   它多看、多问、多比对：业务消息 {sk_p['data']} 条（轻信只有 {gu_p['data']} 条），
   并触发了 {sk_p['overrides']} 次自我更正 —— 先采信了发布平台的「无 DDL」，拿到审计的
   逐条执行记录后才改口。而轻信策略里这个数字是 {gu_p['overrides']}：**它连改口的机会都没有。**
""")
    print("注：本实验的设计目标就是让「对等一定更好」这句话站不住 ——")
    print("    对等通信的价值不在「更对」，而在「更省」，以及让关键信息绕开中心。")

    from peer_agents.dirty_report import write_dirty_html, write_dirty_json

    html_path = write_dirty_html(runs, HUB, ROOT / "out" / "dirty.html")
    json_path = write_dirty_json(runs, HUB, ROOT / "out" / "dirty.json")
    print()
    print(f"HTML 报告：{html_path}")
    print(f"认知账本：{json_path}")


if __name__ == "__main__":
    main()
