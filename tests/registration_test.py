#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 07 12:44:23 2022

@author_ dhaneor
"""
import asyncio
import json
import logging
import sys
import time
import zmq
import zmq.asyncio

from collections import namedtuple
from pprint import pprint
from os.path import dirname as dir
from typing import NamedTuple

sys.path.append(dir(dir(__file__)))
sys.path.append(dir(dir(dir(__file__))))

import registration as reg  # noqa: E402
from base_config import BaseConfig  # noqa E402
from fukujou import curve  # noqa: E402

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
)
handler.setFormatter(formatter)

# --------------------------------------------------------------------------------------
SEND_ADDR = "inproc://reg_test"
RECV_ADDR = "tcp://127.0.0.1:8001"   # "inproc://reg_test"

scroll = reg.Scroll(
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

config = BaseConfig("kucoin", ["spot"], [])
config.rgstr_with = ["streamer"]
config._endpoints = {
    "registration": "tcp://127.0.0.1:5600", "publisher": "tcp://127.0.0.1:5601"
}

Keys = namedtuple("Keys", ["public", "private"])
amanya_keys = Keys(*curve.generate_curve_key_pair())


class CSR(NamedTuple):
    name = "Amanya"
    public_key = amanya_keys.public
    endpoints = {"requests": RECV_ADDR}
    endpoint = RECV_ADDR


# ======================================================================================
def test_scroll_ttl() -> None:
    s = scroll
    s.ttl = time.time() + reg.TTL

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


async def mock_csr(ctx: zmq.asyncio.Context) -> None:
    streamer = BaseConfig("kucoin", ["spot"], [])
    streamer._endpoints = {
        "registration": "tcp://127.0.0.1:5600",
        "publisher": "tcp://127.0.0.1:5601"
    }
    streamer.rgstr_with = ["collector"]

    # await asyncio.sleep(12)

    with zmq.asyncio.Socket(ctx, zmq.ROUTER) as sock:
        sock.curve_secretkey = amanya_keys.private.encode("ascii")
        sock.curve_publickey = amanya_keys.public.encode("ascii")
        sock.curve_server = True
        sock.bind(RECV_ADDR)

        poller = zmq.asyncio.Poller()
        poller.register(sock, zmq.POLLIN)

        logger.debug("mock CSR started: OK")

        while True:
            try:
                events = dict(await poller.poll())

                if events:
                    logger.debug(events)

                if sock in events:
                    req = await sock.recv_multipart()
                    key, msg = req[0], req[1:]
                    logger.debug("received request: %s" % msg)
                    await sock.send_multipart(
                        [
                            key,
                            b"ADD",
                            streamer.as_json().encode(),
                            streamer.as_json().encode()
                        ]
                    )
            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                logger.error(e, exc_info=1)
                break
            except Exception as e:
                logger.error("unexpected error: %s", e, exc_info=1)
                break

        logger.debug("mock CSR stopped: OK")


# --------------------------------------------------------------------------------------
async def test_monitor_registration() -> None:
    ctx = zmq.asyncio.Context()

    reg_sock = ctx.socket(zmq.ROUTER)
    reg_sock.bind(SEND_ADDR)
    client_sock = ctx.socket(zmq.DEALER)
    client_sock.connect(RECV_ADDR)

    monitor = asyncio.create_task(
        reg.monitor_registration(reg_sock, callbacks=[callback])
    )

    for _ in range(2):
        await scroll.send(client_sock)
        await asyncio.sleep(1)

    monitor.cancel()

    await asyncio.gather(monitor)


async def test_vigilante():
    ctx = zmq.asyncio.Context()
    csr = CSR()
    csr_task = asyncio.create_task(mock_csr(ctx))
    v = reg.Vigilante(context=ctx, config=config, csr=csr, on_new=[])

    logger.debug(v)

    await v.start()
    await v.get_initial_data()

    await asyncio.sleep(0.1)

    # logger.debug("Vigilante services: %s", v.services)
    await v.stop()
    csr_task.cancel()
    await asyncio.gather(csr_task)


async def test_rawi():
    ctx = zmq.asyncio.Context()

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(reg.exception_handler)

    async def log(x):
        logger.debug(x)
        await asyncio.sleep(1)

    # prepare a configuration for our test server
    class Server(BaseConfig):

        def __init__(self):
            super().__init__()
            self.name = "server"
            self.service_type = 'collector'
            self.exchange = "kucoin"
            self.actions = []
            self.queues = []
            self.latency_tracker = None
            self._endpoints = {
                "registration": "tcp://*:11000",
                "publisher": "tcp://127.0.0:5556"
            }
            self.publisher_addr = self._endpoints["publisher"]

    # prepare a configuration for our test client
    class Client(BaseConfig):

        def __init__(self):
            super().__init__()
            self.name = "client"
            self.service_type = 'streamer'
            self.exchange = "kucoin"
            self.actions = []
            self.queues = []
            self.latency_tracker = None
            self._endpoints = {
                "registration": "tcp://*:5560",
                "publisher": "tcp://127.0.0:5561"
            }
            self.publisher_addr = self._endpoints["publisher"]
            self._rgstr_max_errors = 1

    cnf_srv = Server()
    cnf_cli = Client()

    # define a function that returns the registration information
    def get_rgstr_info():

        class C:
            endpoints = {"registration": "tcp://127.0.0.1:11000"}
            public_key = cnf_srv.public_key

            def __repr__(self):
                return f"C(endpoint={self.endpoint}, public_key={self.public_key})"

        return C()

    logger.info("server config: %s", cnf_srv.as_dict())
    logger.info("client config: %s", cnf_cli.as_dict())
    logger.info("registration info: %s", get_rgstr_info())

    # start the Rawi instance
    server = reg.Rawi(ctx, cnf_srv, get_rgstr_info, [log])
    client = reg.Rawi(ctx, cnf_cli, get_rgstr_info, [log])

    # try registering with the Rawi instance
    await server.start()
    await asyncio.sleep(1)
    try:
        await client.register()
    except reg.RegistrationError as e:
        logger.error(e)
    await asyncio.sleep(2)
    await client.stop()
    await server.stop()

if __name__ == '__main__':
    asyncio.run(test_vigilante())
    # test_scroll_ttl()
    # test_scroll_from_config()
