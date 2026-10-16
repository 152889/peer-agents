"""传输层：把帧送上线、从线上取回帧。

**这里面一个线程都没有。** 这是刻意的，不是偷懒。

README §5 把「单线程轮转调度、没有并发、竞态未测」列为一条**未验证**边界。
如果这一层顺手开几个后台线程去收发，就等于在一条自己声明「没测过」的
边界上悄悄盖了房子 —— 而项目中所有难查的 bug 都是从「悄悄」开始的。

所以这里的模型是：**宿主驱动、同步往返。**
    宿主：``think`` --> 对端
    对端：读到 frame，算完，写回一帧
    宿主：读回 ``say``

只在一个地方阻塞，就永远不会有竞态。等真要做并发版（Roadmap 里那条
asyncio），改动集中在这一层，线协议与远端大脑都不用动。

超时是个**结构性**属性而不是可选项
----------------------------------
``Peer`` 自带 ``default_timeout``，调用方不显式传超时也会生效。一个沉默的
对端不应该有能力把调度器永久挂住 —— 这和「拒绝静默降级」是同一条底线：
**每一种失败都得有归宿**，超时的归宿是抛出去、由 Runtime 记成一次故障。

分帧为什么自己写
----------------
没用 ``sock.makefile("rb")``：它自带缓冲，而「缓冲遇上超时」的语义是实现
细节 —— 超时后缓冲区里剩下什么、丢没丢，都不是契约。这个协议要能被第三种
语言照着重写，读取语义就必须写死在纸面上（见 ``docs/PROTOCOL.md``）。
"""

from __future__ import annotations

import contextlib
import socket
from collections.abc import Callable

from .wire import (
    F_ERROR,
    F_HELLO,
    F_WELCOME,
    MAX_LINE_BYTES,
    PROTO_VERSION,
    FrameTooLarge,
    Hello,
    MalformedFrame,
    PermissionDenied,
    WireError,
    decode_line,
    encode_frame,
    make_bye,
    make_error,
    parse_hello,
)

DEFAULT_TIMEOUT = 10.0
RECV_CHUNK = 65536

TraceFn = Callable[[str, dict], None]


def _noop(_direction: str, _payload: dict) -> None:
    pass


class FrameReader:
    """从 socket 上按行取帧。行分隔语义完全显式，不依赖任何缓冲实现。"""

    def __init__(self, sock: socket.socket, max_line: int = MAX_LINE_BYTES) -> None:
        self.sock = sock
        self.max_line = max_line
        self.buf = b""

    def readline(self) -> dict | None:
        """返回下一帧；对端干净收尾时返回 ``None``。

        抛 ``TimeoutError`` 表示在超时内没凑齐一帧 —— 这与「对端断开」是
        两回事，调用方必须能分开处置，所以绝不把它们合并成一个返回值。
        """
        while True:
            nl = self.buf.find(b"\n")
            if nl >= 0:
                line, self.buf = self.buf[:nl], self.buf[nl + 1:]
                if not line.strip():
                    continue                      # 空行当心跳，跳过
                return decode_line(line)
            if len(self.buf) > self.max_line:
                raise FrameTooLarge(
                    f"累计 {len(self.buf)} 字节仍未见到换行，超过单帧上限 {self.max_line}"
                )
            chunk = self.sock.recv(RECV_CHUNK)
            if not chunk:
                if self.buf.strip():
                    raise MalformedFrame("对端在行中间断开，留下半截报文")
                return None                       # 干净 EOF
            self.buf += chunk


class Peer:
    """一条已经建立的对端连接。发一帧、收一帧，就这些。"""

    def __init__(
        self,
        sock: socket.socket,
        *,
        trace: TraceFn | None = None,
        label: str = "",
        default_timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.sock = sock
        self.reader = FrameReader(sock)
        self.trace = trace or _noop
        self.label = label
        self.default_timeout = default_timeout
        self.closed = False
        # 链路**已经坏了**（对端断开 / 套接字报错），而不是"这一轮没赶上"。
        # 超时不置这个标志 —— 超时是暂时的，坏掉的连接不是。
        # 这个区分由远端大脑用来决定「我还欠着东西吗」，见 remote.py。
        self.broken = False
        self.hello: Hello | None = None

    # -- 收发 -------------------------------------------------------------
    def send(self, payload: dict) -> None:
        if self.closed:
            raise WireError("连接已关闭，不能再发帧")
        self.trace("->", payload)
        try:
            self.sock.sendall(encode_frame(payload))
        except TimeoutError:
            # 注意顺序：``TimeoutError`` 是 ``OSError`` 的子类，
            # 先兜住它，否则一次超时会被误判成「链路坏了」。
            raise
        except OSError:
            self.broken = True
            raise

    def recv(self, timeout: float | None = None) -> dict | None:
        t = self.default_timeout if timeout is None else timeout
        old = self.sock.gettimeout()
        self.sock.settimeout(t)
        try:
            frame = self.reader.readline()
        except TimeoutError:
            raise
        except OSError:
            self.broken = True
            raise
        finally:
            self.sock.settimeout(old)
        if frame is None:
            self.broken = True
        else:
            self.trace("<-", frame)
            if frame.get("t") == F_ERROR:
                # 对端明确拒收了我们发的帧。把这层意思翻成人话再抛出去，
                # 而不是让调用方去猜「为什么下一帧对不上」。
                raise WireError(f"对端拒收：{frame.get('reason')}")
        return frame

    def expect(self, frame_type: str, timeout: float | None = None) -> dict:
        frame = self.recv(timeout)
        if frame is None:
            raise WireError(f"期待 {frame_type} 帧，但对端提前断开了")
        got = frame.get("t")
        if got != frame_type:
            raise MalformedFrame(f"期待 {frame_type} 帧，收到 {got!r}")
        return frame

    # -- 收尾 -------------------------------------------------------------
    def say_bye(self) -> None:
        """礼貌收工。发不出去也无所谓 —— 对端大概率已经走了。"""
        if self.closed:
            return
        with contextlib.suppress(OSError):
            self.send(make_bye())

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def __enter__(self) -> Peer:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.say_bye()
        self.close()

    def __repr__(self) -> str:  # pragma: no cover
        state = "closed" if self.closed else "open"
        return f"<Peer {self.label or '?'} {state}>"


class WireServer:
    """宿主一侧的监听端。

    只用 ``accept()`` 收一个对端，而且同步等 —— 因为这一版的设计就是
    「一次会话、一个远端大脑」。等真要收多个，这里是唯一要动的地方。
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        trace: TraceFn | None = None,
        default_timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.host = host
        self._port = port
        self.trace = trace or _noop
        self.default_timeout = default_timeout
        self.sock: socket.socket | None = None

    def listen(self) -> WireServer:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self._port))
        s.listen(4)
        self.sock = s
        self._port = int(s.getsockname()[1])
        return self

    @property
    def port(self) -> int:
        """绑定后才有真实端口 —— 传 0 时由内核分配，这里读回来。"""
        return self._port

    @property
    def address(self) -> tuple[str, int]:
        return self.host, self._port

    def accept(self, *, timeout: float = DEFAULT_TIMEOUT,
               expect_agent: str | None = None) -> tuple[Peer, Hello]:
        """等一个对端连上来并完成握手。

        ``expect_agent`` 不是可选的礼貌 —— 它是**身份绑定**：会话预先决定
        「这个座位坐的是谁」，对端不能自己挑一个名字坐进来。少了它，任意
        进程都能冒充名册里的任何一个成员。
        """
        if self.sock is None:
            raise WireError("还没 listen()")

        self.sock.settimeout(timeout)
        try:
            conn, addr = self.sock.accept()
        finally:
            self.sock.settimeout(None)

        peer = Peer(conn, trace=self.trace, label=str(addr), default_timeout=self.default_timeout)
        try:
            frame = peer.recv(timeout=timeout)
            if frame is None:
                raise MalformedFrame("对端在握手前就断开了")
            if frame.get("t") != F_HELLO:
                raise MalformedFrame(f"握手期待 {F_HELLO} 帧，收到 {frame.get('t')!r}")
            hello = parse_hello(frame)
            if expect_agent is not None and hello.agent != expect_agent:
                raise PermissionDenied(
                    f"对端自称 {hello.agent!r}，但这个座位属于 {expect_agent!r}"
                )
        except WireError as exc:
            _refuse(peer, exc)
            raise
        except OSError as exc:
            peer.close()
            raise WireError(f"握手读帧失败：{exc}") from exc

        peer.label = hello.agent
        peer.hello = hello
        return peer, hello

    def close(self) -> None:
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def __enter__(self) -> WireServer:
        return self.listen()

    def __exit__(self, *_exc: object) -> None:
        self.close()


def send_welcome(peer: Peer, *, session: str, agents: list[str],
                 directory: dict[str, list[str]], hub: str | None = None) -> None:
    peer.send(
        {
            "t": F_WELCOME,
            "proto": PROTO_VERSION,
            "session": session,
            "agents": list(agents),
            "directory": {k: list(v) for k, v in directory.items()},
            "hub": hub,
        }
    )


def open_connection(
    host: str,
    port: int,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    trace: TraceFn | None = None,
) -> Peer:
    """只建立连接，不做握手。

    单独拆出来，是为了让**宿主和对端能在同一个线程里交替推进** ——
    内核会在 ``accept()`` 被调用之前就把连上的连接排进队列，所以
    「先 connect、再 accept」不会卡住。测试因此完全不需要线程。
    """
    try:
        conn = socket.create_connection((host, port), timeout=timeout)
    except OSError as exc:
        raise WireError(f"连不上 {host}:{port} —— {exc}") from exc
    conn.settimeout(None)
    return Peer(conn, trace=trace, label=f"{host}:{port}", default_timeout=timeout)


def handshake(peer: Peer, hello: dict, *, timeout: float = DEFAULT_TIMEOUT) -> dict:
    """对端一侧：报身份、等 welcome。"""
    try:
        peer.send(hello)
        frame = peer.recv(timeout=timeout)
        if frame is None:
            raise WireError("宿主在握手时断开了")
        if frame.get("t") != F_WELCOME:
            raise MalformedFrame(f"握手期待 {F_WELCOME} 帧，收到 {frame.get('t')!r}")
    except (WireError, OSError):
        peer.close()
        raise
    if isinstance(hello.get("agent"), str):
        peer.label = hello["agent"]
    return frame


def connect(
    host: str,
    port: int,
    hello: dict,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    trace: TraceFn | None = None,
) -> tuple[Peer, dict]:
    """对端一侧的便捷入口：连上去 + 完成握手。"""
    peer = open_connection(host, port, timeout=timeout, trace=trace)
    return peer, handshake(peer, hello, timeout=timeout)


def expect_type(frame: dict, wanted: str, want_alt: str | None = None) -> dict:
    """类型断言的小工具，供对端实现与测试共用。"""
    got = frame.get("t")
    if got == wanted or (want_alt is not None and got == want_alt):
        return frame
    raise MalformedFrame(f"期待 {wanted}{'/' + want_alt if want_alt else ''} 帧，收到 {got!r}")


def _refuse(peer: Peer, exc: WireError) -> None:
    """拒绝对端时，**把原因说出来**再断开。"""
    with contextlib.suppress(OSError):
        peer.send(make_error(str(exc)))
    peer.close()


__all__ = [
    "DEFAULT_TIMEOUT",
    "FrameReader",
    "Peer",
    "TraceFn",
    "WireServer",
    "connect",
    "expect_type",
    "handshake",
    "open_connection",
    "send_welcome",
]
