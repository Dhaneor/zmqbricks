#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides reusable functions related to heartbeating in a ZeroMQ system.

These functions are meant to be building blocks that can be used in
implementations of actual ZeroMq components.



Message protocols:

All functions are tailored to use a specific message protocol for
heartbeats. Those that wait for heartbeats will log an error  and
proceed if the client sends anything else than the following:

>>> heartbeat_socket.send_multipart(
>>>    [
>>>        uid.encode(),  # unique id
>>>        name.encode(),  # arbitrary name, just for logging purposes
>>>        "hoy".encode(),  # arbitrary message
>>>        "farmer".encode(),  # service type
           time.time(),  # time this message was sent
>>>    ]
>>> )

To make sure this works, the client can also use the Scroll class.


Classes:
    Scroll
        Represents a hearbeat message, has an integrated send method

Functions:
    get_kinsman_from_heartbeat_message
        Builds a Kinsman class from a (valid) heartbeat message.

    check_kinsfolk
        Checks 'liveness' of connected peers, removes them if they are 'dead'.

    handle_heartbeat_message
        Processes a heartbeat message.

    monitor_hb
        Runs an independant loop and monitors a heartbeat socket.

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import json
import logging
import time
import zmq
import zmq.asyncio

from dataclasses import dataclass, field
from functools import partial
from typing import Optional, Sequence, Callable, Mapping, Any, TypeVar, Coroutine

from zmqbricks.kinsfolk import KinsmanT  # noqa: F401, E402
from zmqbricks.base_config import ConfigT  # noqa: F401, E402
from zmqbricks.util.async_timer_with_reset import create_timer  # noqa: F401, E402

logger = logging.getLogger("main.heartbeat")
logger.setLevel(logging.INFO)


# constants
HB_GREETING = "HOY"
HB_REPLY = "HOY-HOY"

DEFAULT_PUB_TOPIC = "heartbeat"
DEFAULT_ENCODING = "utf-8"

ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)
PayloadDataT = TypeVar("PayloadDataT", bound=Mapping[str, Any])
PayloadT = PayloadDataT | Coroutine[PayloadDataT, None, None]
SocketT = TypeVar("SocketT", bound=zmq.Socket)


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


# ======================================================================================
@dataclass
class HeartbeatMessage:
    """Represents a heartbeat message.

    Attributes
    ----------
    uid: str
        unique identifier of the sender
    name: str
        name of the sender
    socket: zmq.Socket
        Socket for sending the heartbeat message, must be bound or connected.
    greeting: Optional[str]
        Greeting message.
    sequence: int
        Sequence number of the message
    timestamp: float
        time when the message was sent, will be set automatically
    payload: Optional[PayloadT]
        Additional payload to send with the heartbeat message.
        This can be a JSON serializable object or a coroutine
        that returns one.
    encoding: Optional[str]
        Encoding to be used for this message
    _sender_id: Optional[bytes]
        Routing id of the sender, only used for ROUTER sockets.
    """

    uid: str
    name: str
    socket: SocketT
    greeting: str = HB_GREETING
    sequence: int = 0
    timestamp: float = field(default_factory=lambda: time.time())
    payload: Optional[PayloadT] = field(default_factory=dict)
    encoding: Optional[str] = DEFAULT_ENCODING
    _sender_id: Optional[bytes] = None

    # def __repr__(self) -> str:
    #     return f"{self.name} says {self.greeting}"

    # def __str__(self) -> str:
    #     return f"{self.name} says {self.greeting}"

    def _to_multipart(self) -> Sequence[bytes]:

        return [
            DEFAULT_PUB_TOPIC.encode(self.encoding),  # topic
            json.dumps(self.payload).encode(self.encoding),
            self.uid.encode(self.encoding),
            self.name.encode(self.encoding),
            self.greeting.encode(self.encoding),
            str(time.time()).encode(self.encoding),
        ]

    async def send(self) -> None:
        """Send the heartbeat message."""
        msg = self._to_multipart()
        self.socket.send_multipart(msg)

    @staticmethod
    def from_msg(
        msg: Sequence[bytes],
        socket: Optional[SocketT] = None
    ) -> "HeartbeatMessage":
        # messages received by a ROUTER socket contain an additional
        # frame with the sender's identity, so we need to distinguish
        # here
        if socket and socket_type(socket) == "ROUTER":
            msg_start_frame, sender_id = 2, msg[0].decode()
        else:
            msg_start_frame, sender_id = 1, None

        return HeartbeatMessage(
            payload=json.loads(msg[msg_start_frame].decode(DEFAULT_ENCODING)),
            uid=msg[msg_start_frame + 1].decode(DEFAULT_ENCODING),
            name=msg[msg_start_frame + 2].decode(DEFAULT_ENCODING),
            socket=socket,
            greeting=msg[msg_start_frame + 3].decode(DEFAULT_ENCODING),
            timestamp=float(msg[msg_start_frame + 4].decode(DEFAULT_ENCODING)),
            encoding=DEFAULT_ENCODING,
            _sender_id=sender_id,
        )


async def recv_hb(
    socket: zmq.Socket,
    actions: Optional[Sequence[Callable]] = None,
    queues: Optional[Sequence[asyncio.Queue]] = None,
    latency_tracker: Optional[Coroutine[float, None, None]] = None,
) -> None:
    """Monitors a heartbeat socket, runs an independent loop.

    This function is intended to be run as an asyncio task and to totally
    decouple the monitoring of the heartbeat socket from the main loop.

    Parameters
    ----------
    socket
        The ZeroMQ socket to monitor, must be ready-to-use

    hb_interval_seconds
        The interval in seconds between each heartbeat message.
        NOTE: May equal the expected heartbeat interval, but needs
        to be significacntl lower, if we expect heartbeats from
        multiple peers.

    actions
        One or more function to execute when a heartbeat message
        is received. The receiving function shouldn't be surprised
        to receive the uid & payload of the heartbeat message.

    queues: Optional[Sequence[asyncio.Queue]]
        Asyncio queues to use for sending heartbeat messages.

    latency_tracker: Optional[Coroutine[float]]
        A coroutine that tracks latencies for messages.
    """
    if socket_type(socket) in ("SUB", "XSUB"):
        socket.setsockopt(zmq.SUBSCRIBE, DEFAULT_PUB_TOPIC.encode())

    logger.info("heartbeat monitoring started ...")

    while True:
        try:
            hb_msg = HeartbeatMessage.from_msg(await socket.recv_multipart())

            # inform latency tracker, if provided
            if latency_tracker:
                await latency_tracker(time.time() - hb_msg.timestamp)

            # execute action(s), if provided
            if actions:
                for action in actions:
                    try:
                        await action(hb_msg.uid, hb_msg.payload)
                    except Exception as e:
                        logger.exception(e)

            # publish message to queue(s), if provided
            if queues:
                for queue in queues:
                    await queue.put_nowait((hb_msg.uid, hb_msg.payload))

            logger.debug("'%s!' from: %s", hb_msg.greeting, hb_msg.name)

            if not (actions or queues):
                logger.warning("noone will know about, no actions or queues to publish")

        except zmq.ZMQError as e:
            logger.error("ZMQ  error: %s", e)
        except asyncio.CancelledError:
            logger.debug("heartbeat monitoring cancelled")
            break
        except Exception as e:
            logger.error("unexpected error: %s", e)
            logger.exception(e)

    logger.info("heartbeat monitoring stopped: OK")


# ======================================================================================
async def send_hb(
    socket: zmq.Socket,
    uid: str,
    name: str,
    payload: Optional[PayloadT] = None
) -> None:
    """Send a heartbeat message to the publisher.

    Parameters
    ----------
    socket
        A ZeroMQ socket of type: PUB, XPUB, DEALER, REP, PUSH, STREAM.
        REQ socket will also work, _if_ it is in the right state = not
        waiting for a response.

    uid
        The unique identifier of the sender.

    name:
        The name of the sender.

    payload: Optional[PayloadT]
        Additional payload to send with the heartbeat message, if required.
    """
    if asyncio.iscoroutinefunction(payload):
        logger.debug("getting payload from coroutine ...")
        payload = await payload()

    hb_msg = HeartbeatMessage(uid, name, socket, payload=payload or {})

    try:
        await hb_msg.send()
    except zmq.ZMQError as e:
        logger.error("ZMQ error: %s", e)
        raise Exception() from e
    else:
        logger.debug("I say '%s!'", hb_msg.greeting)


async def hb_client_req(
    ctx: zmq.asyncio.Context,
    socket: zmq.Socket,
    uid: str,
    name: str,
    payload: Optional[PayloadT] = None,
    hb_interval_seconds: Optional[int] = 1,
):
    sequence, attempts = 0, 0

    while True:
        sequence += 1

        try:
            await HeartbeatMessage(uid, name, socket, payload=payload).send()
            logger.debug("I say '%s!'", HB_GREETING)

            if socket_type(socket) == "REQ":
                attempts += 1

                try:
                    await asyncio.wait_for(
                        socket.recv(), timeout=hb_interval_seconds * 3
                    )
                except asyncio.TimeoutError:
                    if attempts == 0 or attempts % 100 == 0:
                        logger.warning(
                            "Timeout while waiting for heartbeat response "
                            "Retrying every %s seconds...",
                            hb_interval_seconds,
                        )

                    # Close the socket and reinitialize
                    peer_endpoint = socket.getsockopt_string(zmq.LAST_ENDPOINT)
                    socket.close(0)
                    socket = ctx.socket(zmq.REQ)
                    socket.connect(peer_endpoint)
                    await asyncio.sleep(hb_interval_seconds)
                else:
                    attempts = 0

            await asyncio.sleep(hb_interval_seconds)

        except zmq.ZMQError as e:
            logger.error(f"ZMQ error: {e} for message: {sequence}")
            await asyncio.sleep(hb_interval_seconds)
            attempts += 1
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("unexpected error: %s", e)
            logger.exception(e)
            break

    logger.info("heartbeat sending stopped: OK")


async def start_hb_send_task(
    send_fn: Coroutine,
    hb_interval: int
) -> tuple[asyncio.Task, Coroutine]:
    """Start a timer task that triggers sending of heartbeat messages.

    The task will take care of sending a heartbeat message every
    hb_interval seconds.

    NOTE: Will return the task itself and a coroutine that resets
    the timer, which is useful if heartbeat messages are embedded
    in a datastream, where the client treats every data update as
    a heartbeat message.

    Parameters
    ----------
    send_fn : Coroutine
        Coroutine that sends a heartbeat message (with no parameters needed)

    hb_interval : int
        heartbeat interval

    Returns
    -------
    tuple[asyncio.Task, Coroutine[None]]
        - The background task that sends heartbeat messages with hb_interval.
        - A coroutine that resets the timer.
    """
    logger.debug("creating heartbeat send task ...")

    timer_fn, reset_fn = create_timer(hb_interval, send_fn, True)
    return asyncio.create_task(timer_fn(), name="hb_send"), reset_fn


async def start_hb_recv_task(
    socket: zmq.Socket,
    actions: Optional[Sequence[Callable]] = None,
    queues: Optional[Sequence[asyncio.Queue]] = None,
    latency_tracker: Optional[Coroutine[float, None, None]] = None
) -> None:
    return asyncio.create_task(
        recv_hb(socket, actions, queues, latency_tracker), name="hb_rcv"
    )


# ======================================================================================
#                                  HEARTBEAT OOP STYLE                                 #
# ======================================================================================
class Hjarta:
    """Our beating heart.

    Hjarta --> Old Norse word for heart.

    Parameters
    ----------
    ctx : ContextT
        An async ZeroMQ context.
    config : ConfigT
        Configuration parameters.
    on_snd : Optional[Sequence[Coroutine]], optional
        Call these after sending a heartbeat, by default None
    on_rcv : Optional[Sequence[Coroutine]], optional
        Call these after receiving a heartbeat, by default None
    """

    send: Coroutine[None, None, None] = send_hb
    listen: Coroutine[None, None, None] = recv_hb

    def __init__(
        self,
        ctx: ContextT,
        config: ConfigT,
        on_snd: Optional[Sequence[Coroutine]] = None,
        on_rcv: Optional[Sequence[Coroutine]] = None
    ) -> None:
        """Initialize a new Hjarta instance.

        Parameters
        ----------
        ctx : ContextT
            An async ZeroMQ context.
        config : ConfigT
            Configuration parameters.
        on_snd : Optional[Sequence[Coroutine]], optional
            Call these after sending a heartbeat, by default None
        on_rcv : Optional[Sequence[Coroutine]], optional
            Call these after receiving a heartbeat, by default None
        """
        self.ctx = ctx
        self.config = config
        self.tasks: list = [None, None]  # send/listen background tasks

        self.snd_sock: zmq.Socket = self.ctx.socket(zmq.PUB)
        self.snd_sock.bind(config.hb_addr)

        self.rcv_sock = self.ctx.socket(zmq.SUB)
        self.rcv_sock.setsockopt(zmq.SUBSCRIBE, DEFAULT_PUB_TOPIC.encode())

        self.on_snd: list[Coroutine] = on_snd or [],
        self.on_rcv: list[Coroutine] = on_rcv or []

        logger.info("Hjarta initialized: OK")

    async def start(self):
        logger.info("starting heartbeat send/receive tasks...")

        send_fn = partial(
            Hjarta.send, socket=self.snd_sock, uid=self.config.uid, name=self.config.name
        )

        self.tasks[0], self.reset_fn = await start_hb_send_task(
            send_fn, self.config.hb_interval
        )

        self.tasks[1] = await start_hb_recv_task(self.rcv_sock, self.on_rcv)

    async def stop(self):
        for task in self.tasks:
            if task is not None:
                task.cancel()

        asyncio.gather(*self.tasks, return_exceptions=True)

        logger.info("heartbeat tasks stopped: OK")

    async def listen_to(self, endpoint: str) -> None:
        """Listen to a given endpoint for heartbeats.

        Endpoint must be a valid ZMQ endpoint for a PUB/XPUB socket.

        Parameters
        ----------
        endpoint : str
           endoint address
        """
        try:
            self.rcv_sock.connect(endpoint)
            logger.info('Started listening to %s: OK', endpoint)
        except zmq.ZMQError as e:
            logger.error("ZMQ error: %s", e)
        except Exception as e:
            logger.error("unexpected error: %s", e)

    async def stop_listening_to(self, endpoint: str) -> None:
        # self.rcv_sock.setsockopt(zmq.UNSUBSCRIBE, DEFAULT_PUB_TOPIC.encode())
        try:
            self.rcv_sock.disconnect(endpoint)
            logger.info('Stopped listening to %s: OK', endpoint)
        except zmq.ZMQError as e:
            logger.error("ZMQ error: %s", e)
        except Exception as e:
            logger.error("unexpected error: %s", e)

    async def add_hb_sender(self, kinsman: KinsmanT) -> None:
        """Add a new sender to the Hjarta instance.

        Parameters
        ----------
        kinsman : KinsmanT
            A Kinsman instance.
        """
        logger.debug("starting to listen to %s", kinsman.name)
        await self.listen_to(kinsman.endpoints.get("heartbeat", ""))

    async def remove_hb_sender(self, kinsman: KinsmanT) -> None:
        """Remove a sender from the Hjarta instance.

        Parameters
        ----------
        kinsman : KinsmanT
            A Kinsman instance.
        """
        logger.debug("stopping to listen to %s", kinsman.name)
        await self.stop_listening_to(kinsman.endpoints.get("heartbeat", ""))

    async def reset_timer(self):
        logger.debug("resetting heartbeat timer...")
        self.reset_fn()
