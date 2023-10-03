#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides reusable functions related to heartbeating in a ZeroMQ system.

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import zmq
import zmq.asyncio

from typing import Sequence

from sockets import Sockets, SockDef, create_sockets  # noqa E402


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)


# ======================================================================================
async def ccs(ctx: zmq.asyncio.Context, sock_defs: Sequence[SockDef]):
    """The Central Configuration Service

    Parameters
    ----------
    ctx : zmq.asyncio.Context
        A ZeroMQ Context object
    sock_defs : Sequence[SockDef]
        socket definitions to use
    """
    context = ctx or zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()
    sockets = Sockets.from_sock_defs(context, sock_defs, poller)

    logger.debug(sockets)


# ======================================================================================
async def main():
    ctx = zmq.asyncio.Context()
    sock_defs = [
        SockDef("requests", zmq.REP, "bind", "tcp", "*", port=5500),
        SockDef("heartbeat", zmq.REQ, "bind", "tcp", "*", port=5501),
    ]

    try:
        await ccs(ctx, sock_defs)
    except KeyboardInterrupt:
        pass

    logger.info("shutdown complete: OK")

if __name__ == "__main__":
    asyncio.run(main())
