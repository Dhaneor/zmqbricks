#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a standardized way to register services with each other.

It is possible to use the functions for a functional approach, or
the Rawi class for OOP style registration.

Classes:
    Scroll
        A standardized registration request message.

    ServiceInfoRequest
        A standardized message to request service information
        from the Central Service Registry (CSR).

    Rawi
        registration component as class

Functions:
    register
        function to register with a particular service

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

from collections import deque
from dataclasses import dataclass
from time import time
from typing import (
    Coroutine,
    Optional,
    Sequence,
    Mapping,
    TypeAlias,
    Any,
    TypeVar,
)

from zmqbricks.request import one_time_request
from zmqbricks.fukujou.nonce import Nonce
from zmqbricks.fukujou.curve import generate_curve_key_pair
from zmqbricks.base_config import ConfigT
from zmqbricks.exceptions import (
    MissingTtlError,
    ExpiredTtlError,
    MissingNonceError,
    DuplicateNonceError,
    EmptyMessageError,
    RegistrationError,
    MaxRetriesReached,
    BadScrollError,
)

logger = logging.getLogger("main.registration")
logger.setLevel(logging.DEBUG)

DEFAULT_RGSTR_TIMEOUT = 20  # seconds
DEFAULT_RGSTR_LOG_INTERVAL = 900  # log the same message again after (secs)
DEFAULT_RGSTR_MAX_ERRORS = 10  # maximum number of registration errors

ENCODING = "utf-8"
TTL = 10  # time-to-live for a scroll (seconds)
MAX_LEN_NONCE_CACHE = 1000  # how many nonces to store for comparison
MAX_LEN_CSR_CACHE = 1000  # how many pending updates to store (CSR Agent)

KinsfolkT = TypeVar("KinsfolkT", bound=dataclass)
SockT: TypeAlias = zmq.Socket
ContextT: TypeAlias = zmq.asyncio.Context


# ======================================================================================
def get_ttl():
    return int(time() + TTL)


# --------------------------------------------------------------------------------------
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
    certificate: Optional[bytes] = None  # current certificate of the service

    nonce: Optional[bytes] = None  # unique nonce
    ttl: int = 0  # time-to-live for this scroll

    _socket: Optional[zmq.Socket] = None  # socket for use by the send method
    _routing_key: Optional[bytes] = None  # only for replies (for ROUTER socket)!

    def __repr__(self):
        return f"{self.name!r} - {self.service_name!r}"

    def __eq__(self, other) -> bool:
        return all(
            arg
            for arg in (
                getattr(self, var) == getattr(other, var)
                for var in vars(self)
                if not var.startswith("_")
            )
        )

    def __getitem__(self, item: str) -> Any:
        if hasattr(self, item):
            return getattr(self, item)
        raise KeyError(item)

    @property
    def expired(self) -> bool:
        return time() > self.ttl

    def as_dict(self) -> bytes:
        return {
            var: getattr(self, var) for var in vars(self) if not var.startswith("_")
        }

    async def send(
        self, socket: zmq.Socket, routing_key: Optional[bytes] = None
    ) -> None:
        # make sure we have a socket
        if not socket and not self._socket:
            raise ValueError("we cannot send without a socket, can we?!")

        # use provided socket & routing key, if available
        socket = socket or self._socket
        routing_key = routing_key or self._routing_key

        # create msg as json encoded bytes string
        as_dict = self.as_dict()
        as_dict["nonce"] = Nonce.get_nonce()
        as_dict["ttl"] = time() + TTL

        msg_encoded = json.dumps(as_dict).encode()

        # send it (with routing key if socket is a ROUTER socket)
        if not routing_key:
            await socket.send(msg_encoded)
        else:
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

        Raises
        ------
        KeyError
            for missing keys/fields in the dictionary.
        """
        # the easy way ...
        try:
            return Scroll(**as_dict)
        except Exception:
            pass

        # the sure way ...
        must_have = [
            "uid",
            "name",
            "service_name",
            "service_type",
            "endpoints",
            "exchange",
            "markets",
        ]

        # make sure all required keys are present
        if missing := [var for var in must_have if var not in as_dict]:
            raise KeyError("missing keys: %s" % missing)

        return Scroll(
            uid=as_dict.get("uid", None),
            name=as_dict.get("name", None),
            service_name=as_dict.get("service_name", None),
            service_type=as_dict.get("service_type", None),
            endpoints=as_dict.get("endpoints", None),
            exchange=as_dict.get("exchange", None),
            markets=as_dict.get("markets", []),
            description=as_dict.get("description", None),
            version=as_dict.get("version", None),
            public_key=as_dict.get("public_key", None),
            session_key=as_dict.get("session_key", None),
            certificate=as_dict.get("certificate", None),
            nonce=as_dict.get("nonce", None),
            ttl=as_dict.get("ttl", None),
        )

    @staticmethod
    def from_msg(msg: bytes, reply_socket: Optional[zmq.Socket] = None) -> "Scroll":
        """(Re-)Create a registration request from a ZeroMQ message.

        Parameters
        ----------
        msg : bytes
            An (undecoded) ZMQ message to create the registration request from.

        reply_socket : Optional[zmq.Socket]
            The socket to send the reply to.

        Returns
        -------
        Scroll
            The registration request/reply created from the message.

        Raises
        ------
        BadScrollError
            if message is malformed.
        """
        if len(msg) == 2:
            msg_as_dict = json.loads(msg[1].decode(ENCODING))
            msg_as_dict["_routing_key"] = msg[0]
        elif len(msg) == 1:
            msg_as_dict = json.loads(msg.decode(ENCODING))
        else:
            raise BadScrollError(
                "unable to create Scroll from msg of length %s" % len(msg)
            )

        if reply_socket:
            msg_as_dict["_socket"] = reply_socket

        try:
            return Scroll.from_dict(msg_as_dict)
        except KeyError as e:
            logger.exception("unable to create Scroll from: %s --> %s", msg, e)
            raise BadScrollError() from e


ScrollT = TypeVar("ScrollT", bound=Scroll)


@dataclass
class ServiceInfoRequest:
    """A service info request (for use with one_time_request)."""

    service_type: str

    async def send(self, socket):
        socket.send_multipart([self.service_type.encode()])


async def get_rgstr_sock(ctx: ContextT, config: ConfigT) -> SockT:
    logger.info("configuring registration socket at %s", config.rgstr_addr)
    logger.debug("public key: %s", config.public_key)

    rgstr_sock = ctx.socket(zmq.ROUTER)

    if config.encrypt:
        rgstr_sock.curve_secretkey = config.private_key.encode("ascii")
        rgstr_sock.curve_publickey = config.public_key.encode("ascii")
        rgstr_sock.curve_server = True
    else:
        logger.warn("registration socket is NOT ENCRYPTED")

    rgstr_sock.bind(config.rgstr_addr)

    return rgstr_sock


# --------------------------------------------------------------------------------------
# registration monitor helper functions
def check_nonce(request, nonces):
    """Check if a nonce is valid.

    Parameters
    ----------
    nonce : bytes
        The nonce to check.

    nonces : Sequence[bytes]
        The list of nonces to check against.

    Raises
    ------
    MissingNonceError
        Raised if the request does not contain a nonce.

    DuplicateNonceError
        Raised if the request contains a nonce that was already used.
    """
    if not request.nonce:
        raise MissingNonceError(f"{request.name} sent no nonce. message ignored")

    if request.nonce in nonces:
        raise DuplicateNonceError(f"{request.name} reused a nonce. message ignored")

    nonces.append(request.nonce)


def check_ttl(request):
    """Check if a TTL is valid.

    Parameters
    ----------
    request : Scroll
        The scroll to check.

    Raises
    ------
    MissingTtlError
        Raised if the request does not contain a TTL.

    DuplicateTtlError
        Raised if the request contains a TTL that was already used.
    """
    if not request.ttl:
        raise MissingTtlError(f"{request.name}´s scroll has no TTL. message ignored")

    if request.expired:
        raise ExpiredTtlError(f"{request.name}´ scroll TTL expired. message ignored")


# --------------------------------------------------------------------------------------
# registration helper functions
async def decode_rgstr_reply(reply: bytes | list[bytes]) -> dict[str, Any]:
    if isinstance(reply, list):
        reply = reply[0]
    return json.loads(reply.decode())


async def process_rgstr_reply(reply: dict[str, Any]) -> Scroll | None:
    """Process the registration reply, return a Scroll, if possible.

    Parameters
    ----------
    reply : dict[str, Any]
        _description_

    Returns
    -------
    Scroll
        An instance of Scroll

    Raises
    ------
    RegistrationError
        if we got an error message, or an invalid message.
    """
    # build formalized registration reply from dictionary response
    try:
        return Scroll.from_dict(reply)

    except (KeyError, AttributeError):
        # let's see if this is an error message, or just garbage
        if "error" in reply:
            error = reply["error"]
            logger.critical("registration not accepted by peer: %e", error)
        else:
            error = "invalid response"
            logger.critical("invalid registration reply: %s", reply)

    except Exception as e:
        logger.error("unexpected error while processing reply", e, exc_info=1)
        error = e

    else:
        raise RegistrationError(f"registration: FAIL ({error})")


async def _call_them_callbacks(actions: Sequence[Coroutine], payload: Any) -> None:
    """Call one or more callbacks with a payload.

    Parameters
    ----------
    actions : Sequence[Coroutine]
        A sequence of callbacks.
    payload : Any
        The payload/args/kwargs to pass to the callbacks.
    """
    for action in actions:
        try:
            await action(payload)
        except Exception as e:
            logger.critical("action failed: %s --> %s", action, e, exc_info=1)


# --------------------------------------------------------------------------------------
# main function for registering with a peer kinsman
async def register(
    ctx: ContextT,
    config: ConfigT,
    rgstr_info_coro: Coroutine,
    actions: Optional[Sequence[Coroutine]] = None,
    try_forever: Optional[bool] = False,
) -> Scroll:
    """Register with a peer kinsman.

    Parameters
    ----------
    ctx : zmq.Context
        the current ZMQ context

    config : ConfigT
        local component configuration

    rgstr_info_coro : Coroutine
        a coroutine that returns the registration information.

    actions : Optional[Sequence[Coroutine]], optional
        coroutines to call after successful registration, default None

    try_forever : bool, optional
        try forever or until successful, default False

    Returns
    -------
    Scroll
        A Scroll instance from the registration reply

    Raises
    ------
    RegistrationError
        if the registration for all possible peers failed
    """
    # build formalized registration request
    scroll = Scroll.from_dict(config.as_dict())

    # try forever or until successful ...
    while True:
        # rgstr_info_coro may or may not try to to retrieve updated
        # registration information from the Central Service Registry.
        # This makes no sense if we are trying to initially register
        # the caller with the CSR. Then rgstr_info_coro should return
        # the static information for the Central Service Registry.
        while not (peer_scroll := await rgstr_info_coro()):
            asyncio.sleep(10)

        logger.info(
            "... registering with %s - at %s (public key: %s)",
            peer_scroll.service_name,
            peer_scroll.endpoints.get("registration"),
            peer_scroll.public_key,
        )

        try:
            response = await one_time_request(ctx, scroll, peer_scroll, "registration")
            peer_scroll = await process_rgstr_reply(await decode_rgstr_reply(response))
        except ValueError as e:  # missing registration endpoint for peer
            logger.error(e)
            await asyncio.sleep(10)
        except MaxRetriesReached as e:  # peer could not be reached
            logger.error(e)
        except RegistrationError as e:  # invalid registration response
            logger.error(e)
        except Exception as e:  # unexpected error
            logger.error(e, exc_info=1)
            await asyncio.sleep(10)
        else:
            if actions:
                await _call_them_callbacks(actions, peer_scroll)

            logger.info("=" * 120)
            logger.info(
                "registered with %s at %s",
                peer_scroll.service_name,
                peer_scroll.endpoints.get("registration"),
            )
            break
        finally:
            if not try_forever:
                peer_scroll = None
                break

    return peer_scroll


async def send_reply_ok(scroll: Scroll, config: ConfigT) -> None:
    """Sends a reply for an accepted registration request that we received.

    Parameters
    ----------
    scroll : Scroll
        the peer scroll from the registration request
    config: ConfigT
        our configuration
    """
    # if the scroll has no socket, that means that we send the request
    # and this is an answer --> we don't need to send a reply in this case.
    # This is because we only give one set of actions to Vigilante (on_rcv),
    # and maybe this should be split into two, one for registrations that
    # we received, and one for those that we sent. After debugging this
    # callback monster for days ... not now!
    if not scroll._socket:
        return

    logger.info("sending reply to %s", scroll.name)

    reply = Scroll.from_dict(config.as_dict())
    await reply.send(socket=scroll._socket, routing_key=scroll._routing_key)


async def send_reply_fail(scroll: Scroll, error: Optional[str] = None) -> None:
    """Sends a reply for a failed registration request that we received.

    Parameters
    ----------
    scroll : Scroll
        the peer scroll from the registration request
    config: ConfigT
        our configuration
    error: Optional[str], optional
        any error that might have occured while processing the request
    """
    logger.info("sending reply to %s", scroll.name)

    try:
        await scroll._socket.send_multipart(
            [scroll._routing_key, json.dumps({"error": error}).encode("utf-8")]
        )
    except AttributeError as e:
        logger.error(e)
    except zmq.ZMQError as e:
        logger.error(e)
    except Exception as e:
        logger.error("unexpected error: %s", e, exc_info=1)


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
        A ready-to-use ZMQ ROUTER socket.

    callbacks : Optional[Sequence[Coroutine]]
        callback coroutine(s) that can process registration requests.

    queues : Optional[Sequence[asyncio.queues.Queue]]
        asyncio queue(s) for sending registration requests.
    """
    logger.info("registration monitor started: OK")

    nonces = deque(maxlen=MAX_LEN_NONCE_CACHE)

    while True:
        try:
            msg_bytes = await socket.recv_multipart()

            logger.debug("received msg: %s", msg_bytes)

            if len(msg_bytes) == 2 and msg_bytes[1] == b"":
                raise EmptyMessageError()

            request = Scroll.from_msg(msg_bytes, socket)

            logger.info("rgstr req: %s -  %s", request.name, request.service_name)

            # check nonce & TTL of the request
            check_nonce(request, nonces)
            check_ttl(request)

            # perform callbacks for valid request
            if callbacks:
                [await callback(request) for callback in callbacks]

            else:
                logger.warning("no callbacks provided -> request will not be processed")

            # send to queue(s) for valid request
            if queues:
                for queue in queues:
                    queue.put_nowait(request)

            await asyncio.sleep(1)

        except zmq.ZMQError as e:
            logger.error("registration monitor -> zmq error: %s", e, exc_info=1)
            break
        except BadScrollError as e:
            logger.error("bad scroll: %s -> %s", request, e)
        except EmptyMessageError:
            # can happen when clients need to wait the receiver
            # to come back online ... not important
            pass
        except UnicodeDecodeError as e:
            logger.error("unicode decode error: %s", e)
        except (MissingTtlError, ExpiredTtlError) as e:
            logger.error(e)
        except (MissingNonceError, DuplicateNonceError) as e:
            logger.error(e)
        except asyncio.CancelledError:
            break
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.exception("unexpected error: %s", e, exc_info=1)
            # logger.error("exception caused by this msg: %s", msg_bytes)

    logger.info("registration monitor stopped: OK")


class Vigilante:
    """Class that manages registrations for peer kinsman.

    Parameters
    ----------
    ctx : ContextT
        Currently active (async) ZeroMQ context

    config : ConfigT
        The component configuration object

    rgstr_info_coro : Coroutine[None, Scroll]
        A coroutine that returns registration information. It
        should return a Scroll instance from the peer that this
        component should register with.

    actions : Optional[Sequence[Coroutine[Scroll, None, None]]], optional
        Actions to perform after a peer registered with this component.
    """

    def __init__(
        self, config: ConfigT, bootstrap: Coroutine, on_rcv: Coroutine
    ) -> None:
        """Initialize the Rawi (registration) instance.

        Parameters
        ----------
        config : ConfigT
            The component configuration object

        bootstrap : Callable[None, Scroll]
            A function that returns registration information. It
            should return a Scroll instance from the peer that this
            component should register with.

        on_rcv : Coroutine
            Action to perform after a peer registered with this component.
        """
        self.ctx = zmq.asyncio.Context.instance()
        self.config = config
        self.bootstrap = bootstrap
        self.on_rcv = on_rcv

        self.sock: SockT = None
        self.tasks: list = []

        self.services = []  # local registry of theoretically available services
        self.pending = asyncio.Queue()
        self.initialized = False  # set to True when we have initial data

        logger.debug("Vigilante initialized: OK")

    # ..................................................................................
    async def start(self):
        """Start the Vigilante instance"""
        self.sock = await get_rgstr_sock(self.ctx, self.config)
        self.tasks.append(
            asyncio.create_task(monitor_registration(self.sock, [self.on_rcv]))
        )

        # register with the Central Service Registry (Amanya)
        if peer_scroll := await self.register_with_service_registry():
            logger.debug(">>>>> starting tasks now ...")

            self.csr = peer_scroll

            self.tasks.extend(
                [
                    asyncio.create_task(self.get_initial_data()),
                    asyncio.create_task(self.monitor_updates(peer_scroll)),
                    asyncio.create_task(self.process_update()),
                ]
            )

    async def stop(self):
        """Stop the Vigilante instance"""
        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks)

        if self.sock is not None:
            self.sock.close(0)

    # ..................................................................................
    async def get_initial_data(self):
        logger.debug(">>>> GETTING INITIAL DATA")
        for service_type in self.config.rgstr_with:
            if service_type == "csr":
                continue

            while True:
                try:
                    logger.info("requesting service type '%s' from CSR" % service_type)

                    await self.pending.put(
                        await one_time_request(
                            ctx=self.ctx,
                            client=ServiceInfoRequest(service_type),
                            server=self.csr,
                            endpoint="requests",
                        )
                    )
                except BadScrollError as e:
                    logger.error(
                        "unable to get service info for '%s' --> %s" % service_type, e
                    )
                except MaxRetriesReached as e:
                    logger.error(e)
                except Exception as e:
                    logger.error("unexpected error: %s", e, exc_info=1)
                    await asyncio.sleep(5)
                else:
                    break

        self.initialized = True
        logger.debug("CSR agent initialized: OK")

    async def monitor_updates(self, csr_scroll: ScrollT) -> None:
        """Monitors the updates from the Central Service Registry (Amanya)

        Parameters
        ----------
        csr_scroll : ScrollT
            Scroll instance from the Central Service Registry (Amanya)
        """
        # configure subscriber socket
        public_key, private_key = generate_curve_key_pair()
        csr_monitor = self.ctx.socket(zmq.SUB)
        csr_monitor.curve_secretkey = private_key.encode("ascii")
        csr_monitor.curve_publickey = public_key.encode("ascii")
        csr_monitor.curve_serverkey = csr_scroll.public_key.encode("ascii")
        csr_monitor.connect(csr_scroll.endpoints.get("publisher"))

        for service_type in self.config.rgstr_with:
            csr_monitor.setsockopt(zmq.SUBSCRIBE, service_type.encode())

        logger.info("monitor CSR updates started: OK")

        while True:
            try:
                self.pending.put_nowait(await csr_monitor.recv_multipart())
            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                logger.error("csr_agent -> zmq error: %s", e, exc_info=1)
            except ValueError as e:
                logger.error(e)
            except Exception as e:
                logger.exception("unexpected error: %s", e, exc_info=1)

        csr_monitor.close(0)
        logger.info("monitor CSR updates stopped: OK")

    async def process_update(self):
        """Process an update about available services"""

        # wait for all tasks started by this class to initialize
        while not self.initialized:
            await asyncio.sleep(0.1)

        logger.info("starting to process updates from CSR ...")

        while True:
            try:
                msg_bytes = await self.pending.get()

                logger.debug("===> received msg: %s" % msg_bytes)
                logger.debug("=" * 120)

                logger.debug("received msg:\n %s", msg_bytes)

                service_type = msg_bytes[0].decode()
                command, data = msg_bytes[1].decode(), msg_bytes[2:]

                if service_type not in self.config.rgstr_with:
                    # this should never happen, something is wrong
                    # with the CSR if we receive this message
                    logger.warning("unexpected service type: %s" % service_type)
                    continue

                if not data or data[0] == b"":
                    # if there is no data in the message, then we have
                    # no peers to connect to, normal operation will
                    # have to wait for the peer(s) to come back online
                    logger.critical("got empty service info from CSR")
                    continue

                for elem in data:
                    scroll = Scroll.from_dict(json.loads(elem.decode()))

                    if command == "ADD":
                        await self.add_service(scroll)
                    elif command == "REMOVE":
                        await self.remove_service(scroll)
                    else:
                        logger.warning("unexpected command: %s" % command)

            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                logger.error("csr_agent -> zmq error: %s", e, exc_info=1)
            except KeyError as e:
                logger.error("unable to build scroll from dict: %s", e, exc_info=1)
            except BadScrollError as e:
                logger.error(e)
            except json.decoder.JSONDecodeError as e:
                logger.error("unable to decode json: %s", e, exc_info=1)
            except Exception as e:
                logger.exception("unexpected error: %s", e, exc_info=1)

        logger.info("process CSR updates stopped: OK")

    async def add_service(self, scroll: Scroll) -> None:
        # check if the service is already registered, which may happen
        # if the CSR was offline and is back online now
        if not [s for s in self.services if s.uid == scroll.uid]:
            logger.info("ADDING SERVICE: %s -- %s", scroll, scroll.uid)

            self.services.append(scroll)

            # register expects a coroutine that returns a Scroll instance
            async def coro():
                return scroll

            await register(self.ctx, self.config, coro, [self.on_rcv])

        else:
            logger.warning("service already registered: %s", scroll)

    async def remove_service(self, scroll: Scroll) -> None:
        logger.info("removing service: %s", scroll)

        self.services = list(filter(lambda s: s.uid != scroll.uid, self.services))

        # if we lost connection to the CSR, we need to register again
        if scroll.service_type == "Central Configuration Service":
            await self.register_with_service_registry()

    async def register_with_service_registry(self) -> Scroll | None:
        """Register with the Central Service Registry (Amanya)"""

        if not self.config.rgstr_with or "csr" not in self.config.rgstr_with:
            logger.warning("registering with service registry not configured")
            return None

        logger.debug("Starting registration with CSR...")

        # register and try forever or until successful
        scroll = await register(
            ctx=self.ctx,
            config=self.config,
            rgstr_info_coro=self.bootstrap,
            actions=[],
            try_forever=True,
        )

        await self.on_rcv(scroll)
        logger.info("registered with CSR: OK")
        return scroll
