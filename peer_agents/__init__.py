"""peer_agents —— 一个让「子 Agent 之间能互相说话」的最小多 Agent 通信框架。

模块地图
--------
message.py           消息模型（不可变、可寻址、可转发、可多跳）
bus.py               消息总线 + 静态策略 + 动态拓扑路由 + 建链协商
topology.py          动态通信图（需求驱动建链、链路预算、LRU 淘汰、最短路）
blackboard.py        共享黑板（间接通信：服务发现 + 事实沉淀）
agent.py             Agent 壳 + 可插拔大脑（RuleBrain / EchoBrain）
runtime.py           事件循环 + 收敛/死循环/预算三重护栏
scenario_incident.py 演示场景：一次生产故障的分布式定位
llm.py               可选：把规则大脑换成 LLM 大脑
report.py            控制台 trace 与 HTML 对比报告
"""

from .agent import Agent, EchoBrain, RuleBrain, broadcast
from .blackboard import Blackboard
from .bus import MessageBus, full_mesh_policy, star_policy
from .message import (
    ANSWER,
    CONTROL_KINDS,
    FINAL,
    FINDING,
    INFORM,
    LINK_ACCEPT,
    LINK_REJECT,
    LINK_REQUEST,
    QUERY,
    TASK,
    Message,
)
from .runtime import RunResult, Runtime
from .topology import Topology, edge_key

__all__ = [
    "ANSWER",
    "CONTROL_KINDS",
    "FINAL",
    "FINDING",
    "INFORM",
    "LINK_ACCEPT",
    "LINK_REJECT",
    "LINK_REQUEST",
    "QUERY",
    "TASK",
    "Agent",
    "Blackboard",
    "EchoBrain",
    "Message",
    "MessageBus",
    "RuleBrain",
    "RunResult",
    "Runtime",
    "Topology",
    "broadcast",
    "edge_key",
    "full_mesh_policy",
    "star_policy",
]
