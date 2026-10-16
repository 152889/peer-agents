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
        self.registry: dict[str, list[str]] = {}     # topic -> 知情者列表（可能有多个）
        self.facts: dict[str, tuple[str, str]] = {}  # topic -> (内容, 作者)
        self.artifacts: dict[str, str] = {}          # 命名产物（最终交付物等）
        self.round: int = 0                          # 由 Runtime 每轮推进，供大脑判断「新鲜度」

    # -- 服务发现 ---------------------------------------------------------
    def publish_directory(self, mapping: dict[str, list[str]]) -> None:
        self.registry.update({k: list(v) for k, v in mapping.items()})

    def owners(self, topic: str) -> list[str]:
        """返回某 topic 的**全部**知情者。

        真实系统里一个主题往往不止一个知情者，而他们的说法可能不一致 ——
        这正是「只问第一个」和「交叉验证」会分道扬镳的地方。
        """
        return list(self.registry.get(topic, []))

    def owner(self, topic: str) -> str | None:
        owners = self.owners(topic)
        return owners[0] if owners else None

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
