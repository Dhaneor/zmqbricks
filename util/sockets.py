#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sep 21 13:35:23 2023

@author dhaneor
"""
import asyncio
import logging
import random
import zmq
import zmq.asyncio

from dataclasses import dataclass
from typing import Sequence, Any, Literal, Optional

logger = logging.getLogger("main.socket")

SocketType = Literal[
    "REQ",
    "REP",
    "DEALER",
    "ROUTER",
    "PULL",
    "PUSH",
    "PUB",
    "XPUB",
    "SUB",
    "XSUB",
    "STREAM",
]

port_range_for_random_port = range(5600, 5700)


def socket_type(socket: zmq.Socket) -> str:
    """Get the socket type for an already initialized ZMQ socket."""
    socket_type = socket.getsockopt(zmq.TYPE)
    socket_type_str = {
        zmq.PAIR: "PAIR",
        zmq.PUB: "PUB",
        zmq.SUB: "SUB",
        zmq.REQ: "REQ",
        zmq.REP: "REP",
        zmq.DEALER: "DEALER",
        zmq.ROUTER: "ROUTER",
        zmq.PULL: "PULL",
        zmq.PUSH: "PUSH",
        zmq.XPUB: "XPUB",
        zmq.XSUB: "XSUB",
        zmq.STREAM: "STREAM",
    }.get(socket_type, "UNKNOWN")

    return socket_type_str


# --------------------------------------------------------------------------------------
@dataclass
class SockDef:
    """A standardized socket definition.
    Attributes
    ----------
    name: str
        socket name
    type: SocketType
        socket type
    action: Optional[str]
        bind/connect
    transport: Optional[str]
        tcp/inproc/ipc
    ip_addr: Optional[str]
        tcp ip_addr or file descriptor
    port: Optional[str]
        tcp port (onyl for tcp)
    descriptor: Optional[str]
        file descriptor for inproc, ipc
    """

    name: str
    type: Any
    action: Optional[str] = None
    transport: Optional[str] = None
    ip_addr: Optional[str] = None
    port: Optional[int] = 0
    descriptor: Optional[str] = None

    def __post_init__(self) -> None:
        if self.transport is None and self.ip_addr is None:
            self.transport = "inproc"
        elif self.transport is None and self.ip_addr is not None:
            self.transport = "tcp"
        elif self.transport == "tcp" and self.ip_addr is None:
            self.ip_addr = "*"
            self.action = "bind"
        elif self.transport == "tcp" and self.ip_addr is not None:
            if self.port is None:
                self.port = random.choice(list[port_range_for_random_port])

        if self.transport in ("inproc", "ipc"):
            if not self.descriptor:
                raise ValueError(
                    f"desriptor for {self.transport} is missing "
                    f"from socket definition {self}"
                )

    @property
    def endpoint(self) -> str:
        """Get the full endpoint for a socket from a socket definition."""

        if self.transport in ("inproc", "ipc"):
            addr = f"{self.descriptor}"

        elif self.transport == "tcp":
            addr = f"{self.ip_addr}:{self.port}"

        return f"{self.transport}://{addr}"


# --------------------------------------------------------------------------------------
class Sockets:
    """A collection of sockets.

    Attributes
    ----------
    ctx: zmq.Context
        The current ZMQ context.
    poller: zmq.Poller
        The ZMQ poller for the current context.
    sockets: dict[str, zmq.Socket]
        A dictionary of sockets indexed by socket name.

    Methods
    -------
    restart_socket
        Restarts the socket with the given name.
    close_socket
        Closes the socket with the given name
    from_sock_defs
        Creates an instance of this class from socket definitions.
    create_sockets
        Creates a dictionary of sockets indexed by socket name.
    create_socket
        Creates a single socket from a socket definition.

    """

    def __init__(self):
        self._data = {}

        self.ctx: zmq.Context
        self.poller: zmq.Poller
        self.sock_defs: Sequence[SockDef]

    def __repr__(self):
        return "".join([f"\n{key}: {value}" for key, value in self._data.items()])

    def __getattr__(self, key):
        if key in self._data:
            return self._data[key]
        elif key in ("ctx", "poller", "sock_defs"):
            return getattr(self, key)
        else:
            raise AttributeError(f"'DotDict' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        if key == "_data":
            super().__setattr__(key, value)
        elif key in ("ctx", "poller", "sock_defs"):
            super.__setattr__(self, key, value)
        else:
            self._data[key] = value

    def __delattr__(self, key):
        if key == "_data":
            super().__delattr__(key)
        elif key in self._data:
            del self._data[key]
        else:
            raise AttributeError(f"'DotDict' object has no attribute '{key}'")

    # ..................................................................................
    @property
    def sockets(self) -> dict[str, zmq.Socket]:
        """Get all sockets of this instance."""
        return self._data

    def add_socket(self, sock_def: SockDef) -> None:
        """Add a new socket to this instance."""
        socket = self.create_socket(self.ctx, sock_def, self.poller)
        self._data[sock_def.name] = socket

    def restart_socket(self, socket_name: str) -> zmq.Socket:
        """Restart a socket of this instance."""
        socket = self.sockets[socket_name]

        socket.close(1)
        socket = self.ctx.socket(zmq.REQ)

        sock_def = self.sock_defs[socket_name]

        socket.connect(sock_def.endpoint)
        self.poller.register(socket, zmq.POLLIN)

        self.sockets[socket_name] = socket

    def close_sockets(self, linger: int = 0) -> None:
        """Close all sockets of this instance."""
        for name, sock in self.sockets.items():
            logger.debug(f"closing socket {name}")
            sock.close(linger)

    # ..................................................................................
    @classmethod
    def from_sock_defs(
        cls, ctx: zmq.Context, sock_defs: Sequence[SockDef], poller: zmq.Poller
    ) -> object:
        instance = Sockets.__new__(cls)
        instance._data = Sockets.create_sockets(ctx, sock_defs, poller)
        instance.ctx = ctx
        instance.poller = poller
        instance.sock_defs = sock_defs
        return instance

    @staticmethod
    def create_sockets(
        ctx: zmq.Context, sock_defs: Sequence[SockDef], poller: zmq.Poller
    ) -> dict[str, zmq.Socket]:
        return {
            sock_def.name: Sockets.create_socket(ctx, sock_def, poller)
            for sock_def in sock_defs
        }

    @staticmethod
    def create_socket(
        ctx: zmq.Context, sock_def: SockDef, poller: zmq.Poller
    ) -> zmq.Socket:
        """Create a socket based on a socket definition."""

        sock = zmq.Socket(ctx, sock_def.type)

        if sock_def.action == "bind":
            sock.bind(sock_def.endpoint)
        elif sock_def.action == "connect":
            sock.connect(sock_def.endpoint)
        else:
            raise ValueError(f"Invalid socket action: {sock_def.action}")

        if poller:
            poller.register(sock, zmq.POLLIN)

        return sock


# --------------------------------------------------------------------------------------
async def test():
    ctx = zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()

    sockets = [
        SockDef(
            name="test1",
            type=zmq.REP,
            action="bind",
            transport="tcp",
            ip_addr="*",
            port=1234,
        ),
        SockDef(
            name="test2",
            type=zmq.REQ,
            action="connect",
            transport="tcp",
            ip_addr="127.0.0.1",
            port=1234,
        ),
    ]

    try:
        socks = Sockets.from_sock_defs(ctx, sockets, poller)
    except Exception as e:
        print(e)

    print("sockets created: OK")
    print(socks)
    print(dir(socks))

    await asyncio.sleep(2)

    if socks is None:
        print("sockets is None")
    else:
        try:
            socks.close_sockets()
        except Exception as e:
            print(e.__dict__)
        else:
            print("sockets closed: OK")

    ctx.term()


if __name__ == "__main__":
    asyncio.run(test())
