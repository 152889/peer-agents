"""动态通信图 —— 本模块是「子 Agent 互相交流」这条思路的终点形态。

前三个模块解决的是「允许 A 跟 B 说话」；这里解决的是 **「A 该跟谁建立连接」**。

三个关键机制
-----------
1. **需求驱动的建链**：当 A 要发给 B 却发现没有通路时，A 不会去求某个中心
   节点代转，而是直接向 B 发起建链协商。链路是 Agent 为自己的通信需求
   *争取*来的，不是被规划出来的。
2. **链路预算 + LRU 淘汰**：每个 Agent 只能持有 ``budget`` 条直连。
   预算满了还想建新链，就必须淘汰掉最久未使用的那条。这让拓扑成为一个
   会呼吸的结构 —— 长出来，也会被剪掉。
3. **多跳路由**：两个没有直连的 Agent 之间，消息沿着图上的最短路径穿行。
   中间节点只做**转发**（不解读内容、不占上下文），这与「中心枢纽必须
   理解并转述」有本质区别。

控制面与数据面分离
-----------------
建链协商（LINK_REQUEST/ACCEPT/REJECT）走**带外寻址** —— 直接借助通讯录
找到对方地址投递，不受当前拓扑约束；业务消息则严格受拓扑约束。
这对应真实网络里「路由协议与数据转发分离」的设计。
"""

from __future__ import annotations

import math
from collections import deque

Key = tuple[str, str]


def edge_key(a: str, b: str) -> Key:
    return (a, b) if a < b else (b, a)


class Topology:
    def __init__(
        self,
        nodes: list[str],
        budget: int = 3,
        initial_links: list[tuple[str, str]] | None = None,
    ) -> None:
        self.nodes = list(nodes)
        self.budget = budget
        self.links: dict[str, set[str]] = {n: set() for n in nodes}
        self.last_used: dict[Key, int] = {}
        self.clock = 0
        self.evictions = 0
        self.history: list[tuple[int, str, str, str]] = []  # (clock, action, a, b)
        for a, b in initial_links or []:
            self.force_link(a, b)

    # -- 查询 -------------------------------------------------------------
    def has_link(self, a: str, b: str) -> bool:
        return b in self.links.get(a, ())

    def neighbors(self, a: str) -> set[str]:
        return set(self.links.get(a, ()))

    def degree(self) -> dict[str, int]:
        return {n: len(self.links[n]) for n in self.nodes}

    def edges(self) -> list[tuple[str, str]]:
        """返回当前所有链路，**顺序是确定的**。

        ``self.links[a]`` 是 ``set``，直接迭代的顺序取决于字符串哈希，而
        ``PYTHONHASHSEED`` 默认每个进程随机 —— 于是同一条链路在两次运行里
        可能以不同顺序打印出来。``route()`` 早就用 ``sorted()`` 处理了这件事，
        这里当初漏掉了：表现为 ``run_demo.py`` 的「最终：」那一行每次不一样，
        与 README 宣称的「两次同配置运行可逐字节 diff」相矛盾。
        """
        seen: set[Key] = set()
        out: list[tuple[str, str]] = []
        for a in self.nodes:
            for b in sorted(self.links[a]):
                k = edge_key(a, b)
                if k in seen:
                    continue
                seen.add(k)
                out.append((a, b))
        return out

    def traffic_of(self, a: str, b: str, traffic: dict[Key, int]) -> int:
        return traffic.get(edge_key(a, b), 0)

    # -- 变更 -------------------------------------------------------------
    def touch(self, a: str, b: str) -> None:
        """记录一次链路使用，供 LRU 淘汰排序。"""
        self.clock += 1
        self.last_used[edge_key(a, b)] = self.clock

    def force_link(self, a: str, b: str) -> None:
        if a == b:
            return
        self.links[a].add(b)
        self.links[b].add(a)
        self.touch(a, b)
        self.history.append((self.clock, "link", a, b))

    def unlink(self, a: str, b: str) -> None:
        self.links[a].discard(b)
        self.links[b].discard(a)
        self.last_used.pop(edge_key(a, b), None)
        self.history.append((self.clock, "unlink", a, b))

    def try_link(self, a: str, b: str) -> bool:
        """建链。任一方预算已满，就先淘汰它最久未使用的链路。

        返回 False 仅当双方是同一节点。
        """
        if a == b:
            return False
        if self.has_link(a, b):
            return True
        for n in (a, b):
            while len(self.links[n]) >= self.budget and self.links[n]:
                # 末位的 peer 是**并列时的确定性兜底**。``self.links[n]`` 是
                # set，若两条链路 ``last_used`` 相同，``min`` 会返回迭代序里
                # 先遇到的那个 —— 而 set 的顺序随 PYTHONHASHSEED 变。那意味着
                # 「淘汰谁」在不同进程里可能不同，进而改变拓扑演化与实验结论。
                victim = min(
                    self.links[n],
                    key=lambda peer: (self.last_used.get(edge_key(n, peer), 0), peer),
                )
                self.unlink(n, victim)
                self.evictions += 1
        self.force_link(a, b)
        return True

    # -- 路由 -------------------------------------------------------------
    def route(self, src: str, dst: str) -> list[str] | None:
        """BFS 最短路径，返回 [src, ..., dst]；不可达返回 None。"""
        if src not in self.links or dst not in self.links:
            return None
        if src == dst:
            return [src]
        prev: dict[str, str | None] = {src: None}
        q: deque[str] = deque([src])
        while q:
            n = q.popleft()
            for m in sorted(self.links[n]):
                if m in prev:
                    continue
                prev[m] = n
                if m == dst:
                    path = [m]
                    while prev[path[-1]] is not None:
                        path.append(prev[path[-1]])  # type: ignore[arg-type]
                    return list(reversed(path))
                q.append(m)
        return None

    def distance(self, a: str, b: str) -> int | None:
        p = self.route(a, b)
        return None if p is None else len(p) - 1


# ---------------------------------------------------------------------------
# 可视化：把一张拓扑图画成内联 SVG（供 HTML 报告使用）
# ---------------------------------------------------------------------------

SHORT = {"sre": "SRE", "gateway": "网关", "dba": "DB", "deploy": "发布", "ordersvc": "订单"}
ROLE_COLOR = {
    "sre": ("#E6F1FB", "#185FA5", "#0C447C"),
    "gateway": ("#E1F5EE", "#0F6E56", "#085041"),
    "dba": ("#E1F5EE", "#0F6E56", "#085041"),
    "deploy": ("#E1F5EE", "#0F6E56", "#085041"),
    "ordersvc": ("#E1F5EE", "#0F6E56", "#085041"),
}


def _positions(nodes: list[str], cx: float = 160, cy: float = 145, r: float = 95) -> dict[str, tuple[float, float]]:
    out = {}
    n = len(nodes)
    for i, name in enumerate(nodes):
        ang = math.radians(-90 + i * 360 / n)
        out[name] = (cx + r * math.cos(ang), cy + r * math.sin(ang))
    return out


def topology_svg(
    nodes: list[str],
    edges: list[tuple[str, str]],
    traffic: dict[Key, int] | None = None,
    title: str = "",
    note: str = "",
) -> str:
    pos = _positions(nodes)
    traffic = traffic or {}
    used = [e for e in edges if traffic.get(edge_key(*e), 0) > 0]
    idle = [e for e in edges if traffic.get(edge_key(*e), 0) == 0]

    parts: list[str] = []
    for group, color, dashed in ((idle, "#B4B2A9", True), (used, "#0F6E56", False)):
        for a, b in group:
            (x1, y1), (x2, y2) = pos[a], pos[b]
            w = 1.0 if dashed else 1.2 + min(traffic.get(edge_key(a, b), 0), 5) * 0.7
            dash = ' stroke-dasharray="4 4"' if dashed else ""
            parts.append(
                f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                f'stroke="{color}" stroke-width="{w:.1f}"{dash}/>'
            )
            t = traffic.get(edge_key(a, b), 0)
            if t > 0:
                mx, my = (x1 + x2) / 2, (y1 + y2) / 2
                parts.append(
                    f'<text x="{mx:.1f}" y="{my - 3:.1f}" text-anchor="middle" font-size="10" '
                    f'fill="#0F6E56">{t}</text>'
                )

    for name in nodes:
        x, y = pos[name]
        fill, stroke, text = ROLE_COLOR.get(name, ("#F1EFE8", "#888780", "#444441"))
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="22" fill="{fill}" stroke="{stroke}" stroke-width="0.5"/>')
        parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="middle" dominant-baseline="central" '
            f'font-size="11" fill="{text}">{SHORT.get(name, name)}</text>'
        )

    head = (
        f'<text x="160" y="16" text-anchor="middle" font-size="12" font-weight="500" fill="#444441">{title}</text>'
        if title else ""
    )
    tail = (
        f'<text x="160" y="272" text-anchor="middle" font-size="10" fill="#888780">{note}</text>'
        if note else ""
    )
    return (
        '<svg viewBox="0 0 320 280" width="100%" role="img" '
        'xmlns="http://www.w3.org/2000/svg">' + head + "".join(parts) + tail + "</svg>"
    )
