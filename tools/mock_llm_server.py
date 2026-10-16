"""本地 LLM 桩服务 —— 用来验证 LLMBrain 的协议层，而不是模型本身。

为什么需要它
-----------
`LLMBrain` 是整条链路上唯一「没有凭证就跑不了」的部分。但没有真实 API key
并不意味着这段代码无法验证 —— 这里起一个**符合 OpenAI /chat/completions 协议**
的本地服务，让 LLMBrain 完整走一遍：

    渲染 prompt → 发 HTTP → 收响应 → 解析 JSON → 投递消息

于是「协议层是否忠实」这件事就变得可测了。至于「模型跑起来表现如何」，
那是另一个问题，桩服务回答不了，也不该假装能回答。

桩如何决策
---------
它复用与规则模式**完全相同**的 RuleBrain 来做决定。这在测试术语里叫
test double —— 目的是让被测对象（LLMBrain 的传输与解析）处在一个已知正确的
对端之下。如果两端结果一致，就说明协议层没有引入失真。

用法
----
    python tools/mock_llm_server.py          # 独立启动，默认 127.0.0.1:8765
    OPENAI_BASE_URL=http://127.0.0.1:8765/v1 OPENAI_API_KEY=mock \
        python run_llm_demo.py
"""

from __future__ import annotations

import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from peer_agents import message as M  # noqa: E402
from peer_agents.blackboard import Blackboard  # noqa: E402
from peer_agents.scenario_incident import DIRECTORY, _build_brains  # noqa: E402

ID_RE = re.compile(r"你是「(\w+)」")
INBOX_RE = re.compile(r"^\[(\w+):([\w_]+)\]\s+(\S+)\s+→ 你:\s*(.*)$", re.M)

# 与规则模式共用同一套决策逻辑，只是被搬到了 HTTP 边界之外
_BRAINS: dict = {}
_NOTES: dict[str, dict[str, str]] = {}
_CALLS: list[dict] = []


def reset() -> None:
    """把桩恢复到初始状态。

    桩是**有状态**的（大脑记得自己问过什么、答过什么），所以同进程里跑多次
    必须显式重置 —— 否则第二次运行会看到一个「什么都懒得再问」的大脑。
    """
    global _BRAINS, _NOTES, _CALLS
    _BRAINS = _build_brains(active_hub=None, hub_summarize_chars=None)
    # 规则大脑的 sre 是「只写黑板、不发消息」；但真实模型在结案时会把结论**发出去**。
    # 桩必须模拟这个差异，否则 LLM 大脑的产出就没有任何可观测的载体。
    _BRAINS["sre"].report_to = "*"
    _NOTES = {}
    _CALLS = []


reset()


class _AgentStub:
    """最小 Agent 替身：RuleBrain 只依赖 id / notes / done 三样东西。"""

    def __init__(self, agent_id: str) -> None:
        self.id = agent_id
        self.notes = _NOTES.setdefault(agent_id, {})
        self.done = False


def _board() -> Blackboard:
    b = Blackboard()
    b.publish_directory(DIRECTORY)
    return b


def decide(prompt: str) -> list[dict]:
    """把渲染好的 prompt 翻译成一组「要发的消息」。"""
    hit = ID_RE.search(prompt)
    if not hit:
        return []
    agent_id = hit.group(1)
    brain = _BRAINS.get(agent_id)
    if brain is None:
        return []

    stub = _AgentStub(agent_id)
    inbox = [
        M.Message(sender=sender, recipients=(agent_id,), kind=kind, topic=topic, content=content)
        for kind, topic, sender, content in INBOX_RE.findall(prompt)
    ]
    _CALLS.append({"agent": agent_id, "inbox": [(m.kind, m.topic, m.sender) for m in inbox]})

    out = brain.think(stub, inbox, _board())  # type: ignore[attr-defined]
    return [
        {"to": list(x.recipients), "kind": x.kind, "topic": x.topic, "content": x.content}
        for x in out
    ]


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args) -> None:  # 静音，避免污染演示输出
        pass

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        try:
            body = json.loads(raw)
            if not self.headers.get("Authorization"):
                raise ValueError("缺少 Authorization 头")
            prompt = body["messages"][-1]["content"]
            decisions = decide(prompt)
            content = json.dumps(decisions, ensure_ascii=False)
            payload = {
                "id": "mock",
                "model": body.get("model", "mock"),
                "choices": [{"index": 0, "message": {"role": "assistant", "content": content}}],
            }
            status = 200
        except Exception as exc:  # noqa: BLE001
            payload = {"error": {"message": str(exc)}}
            status = 400

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def start(port: int = 0) -> tuple[ThreadingHTTPServer, str]:
    """在后台线程启动桩服务。port=0 表示自动选一个空闲端口。

    返回 (server, base_url)。
    """
    server = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}/v1"


def calls() -> list[dict]:
    return list(_CALLS)


if __name__ == "__main__":
    server, base_url = start(8765)
    print(f"LLM 桩服务已启动：{base_url}")
    print("按 Ctrl+C 停止")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
