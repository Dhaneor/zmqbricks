#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a standardized way to register services with each other.



classes:
    Scroll
        A standardized registration request message.

    RegistrationReply
        A standardized registration reply message

functions:
    monitor_registration
        A function to monitor a registration socket/port.
        Should use a ROUTER socket!

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import json
import logging
import zmq

from dataclasses import dataclass, field
from time import time
from typing import Coroutine, Optional, Sequence, Mapping, TypeAlias

logger = logging.getLogger("main.registration")
logger.setLevel(logging.INFO)

ENCODING = "utf-8"

SockT: TypeAlias = zmq.Socket


async def prepare_send_msg(self) -> bytes:
    as_dict = {var for var in vars(self) if not var.startswith("_") and var is not None}
    return json.dumps(as_dict).encode()


async def send(socket: zmq.Socket) -> None:
    await socket.send(prepare_send_msg())


# --------------------------------------------------------------------------------------
# request/reply classes for standardization of registration messages
@dataclass
class Scroll:
    """A service description for services

    This is meant to be used to register services with each other.
    """

    uid: str  # unique identifier for the service
    name: str  # print name of the service
    service_name: str  # service name as it appears in the service description
    service_type: str  # type of the service
    endpoints: Mapping[str, str]  # endpoints of the service
    exchange: str  # exchange that is handled by the service
    markets: Sequence[str]  # markets that are handled by the service
    description: Optional[str] = None  # more detailed description of the service
    version: Optional[str] = None  # version number
    public_key: Optional[str] = None  # public key of the service
    session_key: Optional[str] = None  # current session key of the service
    _ttl: int = field(default_factory=time)  # time-to-live for this scroll
    _socket: Optional[zmq.Socket] = None  # socket for use by the send method
    _routing_key: Optional[bytes] = None  # only for replies (for ROUTER socket)!

    def prepare_send_msg(self) -> bytes:
        as_dict = {
            var: getattr(self, var)
            for var in vars(self)
            if not var.startswith("_")  # and getattr(self, var) is not None
        }
        return json.dumps(as_dict).encode()

    async def send(
        self,
        socket: zmq.Socket,
        routing_key: Optional[bytes] = None
    ) -> None:
        # make sure we have a socket
        if not socket and not self._socket:
            raise ValueError("no socket provided!")

        # use provided socket & routing key, if available
        socket = self._socket if not socket else socket
        routing_key = self._routing_key if not routing_key else routing_key

        # create msg as json encoded bytes string
        msg_encoded = self.prepare_send_msg()

        # send it (with routing key if socket is a ROUTER socket)
        if not routing_key:
            logger.debug("no routing key provided, sending message")
            await socket.send(msg_encoded)
        else:
            logger.debug("routing key provided, sending message with routing key")
            await socket.send_multipart([routing_key, msg_encoded])

    # ..................................................................................
    @staticmethod
    def from_dict(as_dict: dict) -> "Scroll":
        """(Re-)Create a registration request from a dictionary.

        Parameters
        ----------
        as_dict : dict
            A dictionary to create the registration request from.

        Returns
        -------
        Scroll
            The registration request created from the dictionary.
        """
        # the easy way ...
        try:
            return Scroll(**as_dict)
        except Exception:
            pass

        # the sure way ...
        must_have = [
            "uid", "name", "service_name", "service_type",
            "endpoints", "exchange", "markets"
        ]

        # make sure all required keys are present
        if (missing := [var for var in must_have if var not in as_dict]):
            raise KeyError("Unable to create Scroll, missing keys: %s" % missing)

        return Scroll(
            uid=as_dict.get("uid", None),
            name=as_dict.get("name", None),
            service_name=as_dict.get("service_name", None),
            service_type=as_dict.get("service_type", None),
            endpoints=as_dict.get("endpoints", None),
            exchange=as_dict.get("exchange", None),
            markets=as_dict.get("market", []),
            description=as_dict.get("description", None),
            version=as_dict.get("version", None),
            public_key=as_dict.get("public_key", None),
            session_key=as_dict.get("session_key", None),
        )

    @staticmethod
    def from_msg(msg: bytes, reply_socket: Optional[zmq.Socket] = None) -> "Scroll":
        """(Re-)Create a registration request from a message.

        Parameters
        ----------
        msg : bytes
            A undecoded ZMQ message to create the registration request from.
        reply_socket : Optional[zmq.Socket]
            The socket to send the reply to.

        Returns
        -------
        Scroll
            The registration request/reply created from the message.
        """
        msg_as_dict = json.loads(msg[1].decode(ENCODING))
        msg_as_dict["_routing_key"] = msg[0]

        if reply_socket:
            msg_as_dict["_socket"] = reply_socket

        try:
            return Scroll(**msg_as_dict)
        except (IndexError, AttributeError) as e:
            logger.exception("unable to create Scroll from: %s --> %s", msg, e)


@dataclass(frozen=True)
class RegistrationReply:
    """Represents a registration response for services"""

    uid: str
    name: str
    service_name: Optional[str] = None
    service_type: Optional[str] = None
    endpoints: Optional[Mapping[str, SockT]] = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    description: Optional[str] = None
    command: Optional[str] = None
    reply: Optional[str] = 'OK'
    comment: Optional[str] = None
    public_key: Optional[str] = None
    session_key: Optional[str] = None

    def prepare_send_msg(self) -> bytes:
        as_dict = {
            var: getattr(self, var)
            for var in vars(self)
            if not var.startswith("_")
        }

        logger.debug(as_dict)

        return json.dumps(as_dict).encode()

    async def send(
        self,
        socket: zmq.Socket,
        routing_key: Optional[bytes] = None
    ) -> None:
        if routing_key is not None:
            try:
                await socket.send_multipart([routing_key, self.prepare_send_msg()])
            except Exception as e:
                logger.exception("unable to send registration reply: %s", e)
        else:
            await socket.send(self.prepare_send_msg())

    @staticmethod
    def from_dict(as_dict: dict) -> "Scroll":
        try:
            return RegistrationReply(**as_dict)
        except KeyError as e:
            logger.error(
                "unable to create registration request from: %s --> %s", as_dict, e
            )
        except TypeError as e:
            logger.error(
                "unable to create registration request from: %s --> %s", as_dict, e
            )
        except Exception as e:
            logger.exception("unexpected error: %s --> %s", as_dict, e)

        return None


# --------------------------------------------------------------------------------------
async def monitor_registration(
    socket: zmq.Socket,
    callbacks: Optional[Sequence[Coroutine]] = None,
    queues: Optional[Sequence[asyncio.queues.Queue]] = None,
) -> None:
    """Monitors the registration socket.

    Parameters
    ----------
    socket : zmq.Socket
        A ZMQ socket, preferably a ROUTER socket.
    callbacks : Optional[Sequence[Coroutine]]
        callback coroutines that can process registration requests.
    queues : Optional[Sequence[asyncio.queues.Queue]]
        ZMQ queues for sending registration requests.
    """
    logger.info("registration monitor started: OK")

    while True:
        try:
            msg_bytes = await socket.recv_multipart()
            request = Scroll.from_msg(msg_bytes, socket)

            logger.info(
                "registration request from %s for %s",
                request.name,
                request.service_name
            )

            if callbacks:
                for callback in callbacks:
                    await callback(request)

            if queues:
                for queue in queues:
                    queue.put_nowait(request)

            await asyncio.sleep(1)

        except zmq.ZMQError as e:
            logger.error("registration monitor -> zmq error: %s", e)
        except UnicodeDecodeError as e:
            logger.error("unicode decode error: %s", e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("unexpected error: %s", e)

    logger.info("registration monitor stopped: OK")


if __name__ == "__main__":
    d = {
        "service_type": "collector",
        "service_name": "Collector for KUCOIN SPOT",
        "endpoints": {},
        "uid": "39f531e5-763b-4a1b-863d-b0b548542267",
        "exchange": "kucoin",
        "market": "spot",
        "desc": "",
        "external_ip": "192.168.1.1",
        "name": "Daireann Seamus X.",
        "SUBSCRIBER_ADDR": "tcp://127.0.0.1:5582",
        "PUBLISHER_ADDR": "tcp://127.0.0.1:5583",
        "MGMT_ADDR": "tcp://127.0.0.1:5570",
        "HB_ADDR": "tcp://127.0.0.1:5580"
    }

    # print(
    #     [x for x in dir(Scroll) if not x.startswith("_")]
    # )

    print(Scroll.from_dict(d))
