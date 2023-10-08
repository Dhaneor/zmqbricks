#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 07 12:44:23 2022

@author_ dhaneor
"""
import asyncio
import json
import logging
import os
import sys
import time
import zmq
import zmq.asyncio

from pprint import pprint

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)


# --------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# --------------------------------------------------------------------------------------

from src.zmqbricks import registration as reg  # noqa: E402
from src.data_sources import zmq_config as conf  # noqa E402

SEND_ADDR = "inproc://reg_test"
RECV_ADDR = "inproc://reg_test"

test_msg = reg.Scroll(
    uid="jhfs-950746",
    name='streamer',
    service_name='test service name',
    service_type='streamer',
    endpoints={"publisher": SEND_ADDR, "management": "tcp://127.0.0.1:5600"},
    version='0.0.1',
    exchange='kucoin',
    markets=['spot'],
    description='Kucoin OHLCV streamer',
)

config = conf.Streamer("kucoin", ["spot"], [])


# ======================================================================================
def test_scroll_ttl() -> None:
    s = test_msg

    for _ in range(7):
        logger.debug(
            "scroll ttl left: %d --> expired: %s",
            round(s.ttl - time.time(), 2),
            s.expired
        )
        time.sleep(1)


def test_scroll_from_config():
    as_dict = config.as_dict()
    pprint(as_dict)
    print('-' * 80)
    scroll_pre = reg.Scroll.from_dict(as_dict)
    msg = scroll_pre.prepare_send_msg()
    msg = json.dumps(as_dict).encode()
    print(msg)
    print("=" * 80)
    scroll_post = reg.Scroll.from_msg([b"", msg])
    print(scroll_post)

    try:
        assert scroll_pre == scroll_post
    except AssertionError as e:
        for attr in vars(scroll_pre):
            prea = getattr(scroll_pre, attr)
            posa = getattr(scroll_post, attr)
            if not prea == posa:
                print(f"{attr}: {prea} != {posa}")
        print(e)


# --------------------------------------------------------------------------------------
async def callback(req: reg.Scroll) -> None:
    logger.info("received request: %s", req)


async def main() -> None:
    ctx = zmq.asyncio.Context()

    reg_sock = ctx.socket(zmq.ROUTER)
    reg_sock.bind(SEND_ADDR)
    client_sock = ctx.socket(zmq.DEALER)
    client_sock.connect(RECV_ADDR)

    monitor = asyncio.create_task(
        reg.monitor_registration(reg_sock, callbacks=[callback])
    )

    for _ in range(2):
        await test_msg.send(client_sock)
        await asyncio.sleep(1)

    monitor.cancel()

    await asyncio.gather(monitor)

if __name__ == '__main__':
    # asyncio.run(main())
    # test_scroll_ttl()
    test_scroll_from_config()
