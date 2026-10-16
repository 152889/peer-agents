"""共享黑板：Agent 之间的「间接通信」通道。

消息总线解决的是 *点对点* 通信（A 直接告诉 B）。
但多 Agent 协作还需要另一类通道：*共享状态*。
- 服务发现：谁负责哪个 topic（``registry``）—— 相当于通讯录。
- 事实沉淀：已经查清的结论（``facts``）—— 相当于看板。

两者互补：Agent 先用通讯录定位对象，再用总线直连；或者直接把结论贴上黑板，
让所有感兴趣的人自己取。真实系统里这两种通道几乎总是同时存在。
"""

from __future__ import annotations


class Blackboard:
    def __init__(self) -> None:
        self.registry: dict[str, str] = {}          # topic -> 负责的 agent
        self.facts: dict[str, tuple[str, str]] = {}  # topic -> (内容, 作者)
        self.artifacts: dict[str, str] = {}          # 命名产物（最终交付物等）

    # -- 服务发现 ---------------------------------------------------------
    def publish_directory(self, mapping: dict[str, str]) -> None:
        self.registry.update(mapping)

    def owner(self, topic: str) -> str | None:
        return self.registry.get(topic)

    # -- 事实沉淀 ---------------------------------------------------------
    def publish(self, topic: str, content: str, author: str) -> None:
        self.facts[topic] = (content, author)

    def known(self) -> set[str]:
        return set(self.facts)

    def digest(self, limit: int = 500) -> str:
        if not self.facts:
            return "（黑板为空）"
        lines = [f"- {t}: {c}" for t, (c, _a) in self.facts.items()]
        text = "\n".join(lines)
        return text if len(text) <= limit else text[:limit] + "…"
