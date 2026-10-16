"""用真实大模型协议跑一遍故障定位场景。

它会启动本地桩服务、把 OPENAI_BASE_URL 指向它，然后让 LLMBrain 完整走一遍
HTTP 请求 → 响应解析 → 消息投递 → 拓扑路由。

    python run_llm_demo.py

想接真实模型，不用改这个文件，只要先把桩服务换掉：

    export OPENAI_API_KEY=sk-xxx
    export OPENAI_BASE_URL=https://api.openai.com/v1
    export OPENAI_MODEL=gpt-4o-mini
    python run_llm_demo.py --real
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

from peer_agents import message as M  # noqa: E402
from peer_agents.report import transcript  # noqa: E402
from peer_agents.scenario_incident import GROUND_TRUTH_TOKENS, T_SCOPE, build  # noqa: E402

USE_REAL = "--real" in sys.argv


def bootstrap(runtime) -> None:
    runtime.bus.send(
        M.Message(
            sender="sre",
            recipients=("gateway",),
            kind=M.TASK,
            topic=T_SCOPE,
            content="checkout 接口 P99 由 120ms 劣化到 900ms，请定位你这一侧是否有异常。",
        )
    )


def main() -> None:
    server = None
    if USE_REAL:
        if not os.environ.get("OPENAI_API_KEY"):
            print("✗ 使用 --real 需要先设置 OPENAI_API_KEY")
            return
        print(f"接真实模型：{os.environ.get('OPENAI_BASE_URL', 'https://api.openai.com/v1')}"
              f" / {os.environ.get('OPENAI_MODEL', 'gpt-4o-mini')}")
    else:
        from mock_llm_server import start

        server, base_url = start()
        os.environ["OPENAI_BASE_URL"] = base_url
        os.environ["OPENAI_API_KEY"] = "mock-key"
        os.environ["OPENAI_MODEL"] = "mock-llm"
        print(f"已启动本地 LLM 桩服务：{base_url}")

    print("=" * 78)
    print("LLMBrain 端到端运行 · 场景：checkout P99 劣化根因定位 · 拓扑：对等全连通")
    print("=" * 78)

    try:
        runtime, _board = build(topology="peer", brain_mode="llm")
        bootstrap(runtime)
        result = runtime.run(name="LLMBrain", ground_truth_tokens=GROUND_TRUTH_TOKENS)
    finally:
        if server:
            server.shutdown()

    print()
    print(transcript(result))
    print()
    print(f"终报：{result.final_report}")
    print()
    print(f"业务消息 {result.data_messages} · 轮次 {result.rounds} · 保真度 {result.fidelity * 100:.0f}%")

    # --- 证明协议层真的跑通了 -------------------------------------------------
    brains = [a.brain for a in runtime.agents]
    sent = [b for b in brains if getattr(b, "requests", None)]
    total_requests = sum(len(b.requests) for b in brains)
    print()
    print("─" * 78)
    print(f"实际发出的 HTTP 请求：{total_requests} 次（涉及 {len(sent)} 个 Agent）")
    print(f"每轮都带上了 Authorization 头、model={os.environ.get('OPENAI_MODEL')}")
    if sent:
        sample = sent[0].requests[-1]
        print()
        print(f"抽样：{sent[0].persona[:34]}… 发出的 prompt（末尾 12 行）")
        print("┄" * 78)
        for line in sample["prompt"].rstrip().splitlines()[-12:]:
            print(f"  {line}")
        print("┄" * 78)

    # --- 与规则大脑对照 -------------------------------------------------------
    runtime2, _ = build(topology="peer", brain_mode="rule")
    bootstrap(runtime2)
    rule = runtime2.run(name="RuleBrain", ground_truth_tokens=GROUND_TRUTH_TOKENS)

    # 规则模式的 sre 只把结论登记到黑板；桩服务的 sre 会广播给全体（4 条投递）。
    # 这是决策差异，不是协议差异，比较时必须扣除，否则等于在比两件不同的事。
    fanout = len(runtime.agents) - 1
    adjusted = result.data_messages - fanout

    print()
    print("─" * 78)
    print("与规则大脑对照（同一场景、同一决策逻辑，只换了传输路径）")
    print(f"  RuleBrain：业务消息 {rule.data_messages:<3} 轮次 {rule.rounds}  保真度 {rule.fidelity * 100:.0f}%")
    print(f"  LLMBrain ：业务消息 {result.data_messages:<3} 轮次 {result.rounds}  保真度 {result.fidelity * 100:.0f}%"
          f"  （其中 {fanout} 条是终报广播扇出）")
    print(f"  扣除扇出后：{adjusted} 条，与规则模式{'一致' if adjusted == rule.data_messages else '不一致'}")
    print()
    checks = [
        ("终报文本完全一致", rule.final_report == result.final_report),
        ("协作轮次一致", rule.rounds == result.rounds),
        ("扣除扇出后消息量一致", adjusted == rule.data_messages),
        ("信息保真度一致", rule.fidelity == result.fidelity),
    ]
    for label, ok in checks:
        print(f"  [{'✓' if ok else '✗'}] {label}")
    print()
    print(f"  → 协议层{'未引入任何失真' if all(ok for _l, ok in checks) else '存在差异，需要排查'}")


if __name__ == "__main__":
    main()
