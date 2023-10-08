#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides tests for the heartbeat module.

Created on Sep 10 20:15:20 2023

@author dhaneor
"""
import asyncio
import logging
import os
import sys
import zmq
import zmq.asyncio


logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()

formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
ch.setFormatter(formatter)

logger.addHandler(ch)

# --------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# --------------------------------------------------------------------------------------

import heartbeat as hb  # noqa: F401, E402
import sockets  # noqa: F401, E402


# poller = zmq.Poller()

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
    logger.debug("got a message: %s", msg)


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
        asyncio.create_task(hb.monitor_hb(sockets["monitor"], [mock_callback])),
        asyncio.create_task(hb.hb_client_req(sockets["heartbeat"])),
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


if __name__ == "__main__":
    try:
        asyncio.run(test(False))
    except KeyboardInterrupt:
        pass
