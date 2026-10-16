"""可选的 LLM 大脑：把「规则推理」换成真正的大模型决策。

用法（需要 OpenAI 兼容端点，不依赖任何第三方库）：

    export OPENAI_API_KEY=sk-xxx
    export OPENAI_BASE_URL=https://api.openai.com/v1     # 可选
    export OPENAI_MODEL=gpt-4o-mini                       # 可选

然后在 ``scenario_incident.build`` 里把 ``RuleBrain`` 换成
``LLMBrain(persona=..., skills=...)`` 即可，通信层完全不用改 ——
这正说明「通信基础设施」和「单个 Agent 的智能水平」是正交的两件事。

约束：LLM 必须返回严格的 JSON 数组，每项形如
    {"to": ["dba"], "kind": "QUERY", "topic": "slow_query", "content": "..."}
这样模型就只是在「决定跟谁说什么」，而不能越权改动框架。
"""

from __future__ import annotations

import json
import os
import urllib.request

from .agent import Agent
from .blackboard import Blackboard
from .message import ANSWER, FINAL, Message

SYSTEM_TEMPLATE = """你是多 Agent 协作团队中的一名成员。

# 你的身份
{persona}

# 你掌握的私有信息
{skills}

# 你的私有结论（未核实前不要外传）
{notes}

# 团队共享黑板
{digest}

# 你能做的事
1. 回答他人对你的提问；
2. **如果推进你的判断还需要别人的信息，直接发消息向对方索取**
   （收件人可以填 registry 里对应的负责人，也可以填 "*" 广播询问）；
3. 当你确信已得出可交付结论时，发一条 kind=FINAL 的消息。

# 可用 topic
{topics}

# 输出格式
只输出一个 JSON 数组，不要任何解释文字：
[{{"to": ["agent_id"], "kind": "QUERY|ANSWER|FINDING|FINAL",
   "topic": "topic", "content": "正文"}}]
如果这一轮不需要发任何消息，输出 []。
"""


class LLMBrain:
    def __init__(
        self,
        persona: str,
        skills: dict[str, str] | None = None,
        model: str | None = None,
        temperature: float = 0.2,
    ) -> None:
        self.persona = persona
        self.skills = dict(skills or {})
        self.model = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        self.temperature = temperature
        self.base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        self.requests: list[dict] = []   # 观测：记录每一次真实发出的请求，便于调试与测试

    # ------------------------------------------------------------------
    def accepts_link(self, requester: str, msg: Message) -> bool:
        """与 RuleBrain 同构的建链策略，保证 LLM 大脑也能参与动态拓扑协商。"""
        if msg.topic in self.skills:
            return True
        return msg.kind in (ANSWER, FINAL) or msg.origin_kind in (ANSWER, FINAL)

    # ------------------------------------------------------------------
    def think(self, agent: Agent, batch: list[Message], board: Blackboard) -> list[Message]:
        out: list[Message] = []
        # 枢纽代投是协议层职责，仍由框架处理，不交给模型
        pending: list[Message] = []
        for m in batch:
            if m.relay_to:
                out.append(m.clone(recipients=(m.relay_to,), relay_to=None, relayed_by=agent.id))
            else:
                pending.append(m)

        prompt = self._render(agent, pending, board)
        for item in self._call(prompt):
            recipients = tuple(item.get("to") or ())
            if not recipients:
                continue
            out.append(
                Message(
                    sender=agent.id,
                    recipients=recipients,
                    kind=item.get("kind", "INFORM"),
                    topic=item.get("topic", "misc"),
                    content=item.get("content", ""),
                )
            )
        return out

    # ------------------------------------------------------------------
    def _render(self, agent: Agent, batch: list[Message], board: Blackboard) -> str:
        inbox = "\n".join(f"[{m.kind}:{m.topic}] {m.sender} → 你: {m.content}" for m in batch) or "（无）"
        topics = "\n".join(f"- {t} → 知情者 {', '.join(o)}" for t, o in board.registry.items())
        system = SYSTEM_TEMPLATE.format(
            persona=self.persona,
            skills=json.dumps(self.skills, ensure_ascii=False, indent=2),
            notes=agent.notes or "（暂无）",
            digest=board.digest(),
            topics=topics,
        )
        return f"{system}\n# 本轮收到的消息\n{inbox}\n"

    def _call(self, prompt: str) -> list[dict]:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("未设置 OPENAI_API_KEY，无法使用 LLMBrain")
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "messages": [{"role": "user", "content": prompt}],
        }
        self.requests.append({"url": f"{self.base_url}/chat/completions", "prompt": prompt, "payload": payload})
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        )
        with urllib.request.urlopen(req, timeout=90) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        return self._parse(body["choices"][0]["message"]["content"])

    @staticmethod
    def _parse(text: str) -> list[dict]:
        """把模型输出解析成指令列表。

        容错是刻意设计的：模型经常会用 ```json 围栏包裹、或者附上解释文字。
        任何一种异常都退化为「本轮不发消息」，而不是让整个协作崩掉 ——
        在一个自主通信的群体里，一个成员失语，比它把错误指令广播出去要好。
        """
        text = (text or "").strip()
        if text.startswith("```"):
            parts = text.split("```")
            if len(parts) > 1:
                text = parts[1]
                text = text[4:] if text.lower().startswith("json") else text
        try:
            data = json.loads(text.strip())
        except json.JSONDecodeError:
            # 再试一次：截取第一个 [ 到最后一个 ] 之间的内容
            start, end = text.find("["), text.rfind("]")
            if start == -1 or end <= start:
                return []
            try:
                data = json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return []
        if not isinstance(data, list):
            return []
        return [item for item in data if isinstance(item, dict) and item.get("to")]
