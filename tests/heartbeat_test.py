#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides tests for the heartbeat module.

Created on Sep 10 20:15:20 2023

@author dhaneor
"""
import asyncio
import logging
import sys
import zmq
import zmq.asyncio

from os.path import dirname as dir

sys.path.append(dir(dir(__file__)))

import heartbeat as hb  # noqa: F401, E402
import util.sockets as sockets  # noqa: F401, E402

# --------------------------------------------------------------------------------------
# configure logger

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()

formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
ch.setFormatter(formatter)

logger.addHandler(ch)

# --------------------------------------------------------------------------------------
# prepare the context

s_defs = [
    sockets.SockDef(
        name="monitor",
        type=zmq.REP,
        action='bind',
        transport="inproc",
        descriptor="monitor",
    ),
    sockets.SockDef(
        name="heartbeat",
        type=zmq.REQ,
        action='connect',
        transport="inproc",
        descriptor="monitor",
    ),
]

# socks = sockets.Sockets.from_sock_defs(ctx=ctx, sock_defs=s_defs, poller=poller)


# --------------------------------------------------------------------------------------
async def mock_callback(msg: str) -> None:
    logger.debug("mock callback got a message: %s", msg)


async def test(pub_sub: bool = False):
    ctx = zmq.asyncio.Context()

    if pub_sub:
        sockets = {"monitor": ctx.socket(zmq.SUB), "heartbeat": ctx.socket(zmq.PUB)}
        sockets["monitor"].setsockopt(zmq.SUBSCRIBE, b"")
        sockets["heartbeat"].setsockopt(zmq.LINGER, 0)
    else:
        sockets = {
            "monitor": ctx.socket(zmq.ROUTER),
            "heartbeat": ctx.socket(zmq.DEALER)
        }

    sockets["heartbeat"].bind("inproc://heartbeat")
    sockets["monitor"].connect("inproc://heartbeat")

    tasks = [
        asyncio.create_task(hb.recv_hb(sockets["monitor"], [mock_callback])),
        asyncio.create_task(
            hb.hb_client_req(
                ctx=ctx, socket=sockets["heartbeat"], uid="test", name="test",
            )
        ),
    ]

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.debug("caught CancelledError ... shutting down")

        running_tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
        ]

        [task.cancel() for task in running_tasks]
        await asyncio.gather(*running_tasks, return_exceptions=True)
    finally:
        [socket.close(0) for socket in sockets.values()]
        logger.debug("sockets closed: OK")
        ctx.term()
        logger.info("shutdown complete: OK")


async def test_hjarta():
    ctx = zmq.asyncio.Context()

    class TestConfig(hb.BaseConfig):

        def __init__(self):
            super().__init__()
            self.name = "test_folk"
            self.hb_interval = 1
            self.socket = zmq.REQ
            self.actions = [mock_callback]
            self.queues = []
            self.latency_tracker = None

            self._endpoints = {
                "heartbeat": "tcp://127.0.0.1:5555",
            }

    h = hb.Hjarta(ctx, TestConfig())

    await h.start()
    await asyncio.sleep(10)
    await h.stop()


if __name__ == "__main__":
    try:
        asyncio.run(test_hjarta())
    except KeyboardInterrupt:
        pass
