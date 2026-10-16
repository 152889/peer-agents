"""带元数据的事实 —— 「极度真实」的第一块地基。

在干净环境里，一条信息就是一句话。在真实环境里，一句话背后至少还有四件事：

    置信度   说这话的人有多确定？（传闻 vs 权威记录）
    时间戳   这是什么时候的快照？（3 天前的 schema 快照可能已经失效）
    证据出处 凭什么这么说？（有过期风险的信息必须能追溯到出处）
    来源     谁说的？（同一个人重复说两遍，不构成交叉验证）

把这些塞进 `Message.content` 里（而不是给总线加字段），是刻意的：
**总线的职责是搬运，不是理解。** 它不该知道「置信度」是什么。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Fact:
    value: str
    confidence: float = 0.8   # 0~1
    as_of: int | None = None  # 快照时间刻度；**None 表示「实时」**
    evidence: str = ""        # 出处说明
    source: str = ""          # 提供者

    # -- 序列化：编成 JSON 塞进消息正文 ------------------------------------
    def encode(self) -> str:
        return json.dumps(
            {"v": self.value, "c": round(self.confidence, 2), "t": self.as_of,
             "e": self.evidence, "s": self.source},
            ensure_ascii=False,
        )

    @staticmethod
    def decode(text: str) -> "Fact | None":
        """解不出结构就返回 None —— 无法识别的信息一律当作「不可采信」。"""
        try:
            d = json.loads(text)
            t = d.get("t")
            return Fact(
                value=str(d["v"]),
                confidence=float(d.get("c", 0.8)),
                as_of=None if t is None else int(t),
                evidence=str(d.get("e", "")),
                source=str(d.get("s", "")),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    # -- 时间 -------------------------------------------------------------
    def stamp(self, now: int) -> "Fact":
        """把「实时」落地成绝对刻度。

        一条「当前情况」在被说出口之前是无所谓时间戳的 —— 只有它离开
        说话人的那一刻起，才可能随时间变旧。所以盖章动作放在**出网时**，
        而不是数据定义时。
        """
        return replace(self, as_of=now) if self.as_of is None else self

    def age(self, now: int) -> int:
        """相对当前轮次有多旧。实时信息恒为 0。"""
        return 0 if self.as_of is None else now - self.as_of

    def is_stale(self, now: int, fresh_within: int) -> bool:
        return self.age(now) > fresh_within

    def is_trusted(self, now: int, threshold: float, fresh_within: int) -> bool:
        """可信 = 置信度够高 **且** 不算过期。两个条件缺一不可。"""
        return self.confidence >= threshold and not self.is_stale(now, fresh_within)

    def contradicts(self, other: "Fact") -> bool:
        return self.value.strip() != other.value.strip()

    def with_source(self, source: str) -> "Fact":
        return replace(self, source=source)

    def brief(self) -> str:
        when = "实时" if self.as_of is None else f"as_of={self.as_of}"
        return f"「{self.value[:26]}…」conf={self.confidence:.2f} {when}"


def best_of(facts: list[Fact]) -> Fact | None:
    """在多个版本里挑最优：先看新鲜度，再看置信度。

    顺序很关键 —— **过期的权威记录，不如新鲜的普通记录。**
    这正是真实环境里最容易翻车的地方。
    """
    if not facts:
        return None
    return max(facts, key=lambda f: (f.as_of, f.confidence))
