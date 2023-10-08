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

from collections import deque
from dataclasses import dataclass
from functools import partial
from time import time
from typing import (
    Coroutine, Optional, Sequence, Mapping, TypeAlias, Callable, Any, TypeVar,
    NamedTuple
)

# from zmqbricks.kinsfolk import KinsfolkT
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
)

logger = logging.getLogger("main.registration")
logger.setLevel(logging.DEBUG)


DEFAULT_RGSTR_TIMEOUT = 10  # seconds
DEFAULT_RGSTR_LOG_INTERVAL = 900  # resend request after (secs)
DEFAULT_RGSTR_MAX_ERRORS = 10  # maximum number of registration errors

ENCODING = "utf-8"
TTL = 5  # time-to-live for a scroll (seconds)
MAX_LEN_NONCE_CACHE = 1000  # how many nonces to store for comparison


KinsfolkT = TypeVar("KinsfolkT", bound=Mapping[str, str])
SockT: TypeAlias = zmq.Socket
# ConfigT: TypeAlias = object
ContextT: TypeAlias = zmq.asyncio.Context


# ======================================================================================
def exception_handler(loop, context):
    logger.error("caught EXCEPTION: %s -> %s", context.get("exception"))


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

    def __eq__(self, other) -> bool:
        return all(
            arg for arg in (
                getattr(self, var) == getattr(other, var)
                for var in vars(self) if not var.startswith("_")
            )
        )

    @property
    def expired(self) -> bool:
        return time() > self.ttl

    def prepare_send_msg(self) -> bytes:
        return {
            var: getattr(self, var) for var in vars(self) if not var.startswith("_")
        }

    async def send(
        self,
        socket: zmq.Socket,
        routing_key: Optional[bytes] = None
    ) -> None:
        # make sure we have a socket
        if not socket and not self._socket:
            raise ValueError("we cannot send without a socket, can we?!")

        # use provided socket & routing key, if available
        socket = socket or self._socket
        routing_key = routing_key or self._routing_key

        # create msg as json encoded bytes string
        as_dict = self.prepare_send_msg()
        as_dict["nonce"] = Nonce.get_nonce()
        as_dict['ttl'] = time() + TTL

        msg_encoded = json.dumps(as_dict).encode()

        logger.debug("sending scroll: %s", msg_encoded)

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
            "uid", "name", "service_name", "service_type",
            "endpoints", "exchange", "markets"
        ]

        # make sure all required keys are present
        if (missing := [var for var in must_have if var not in as_dict]):
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
        """
        msg_as_dict = json.loads(msg[1].decode(ENCODING))
        msg_as_dict["_routing_key"] = msg[0]

        if reply_socket:
            msg_as_dict["_socket"] = reply_socket

        try:
            return Scroll.from_dict(msg_as_dict)
        except KeyError as e:
            logger.exception("unable to create Scroll from: %s --> %s", msg, e)


# --------------------------------------------------------------------------------------
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
        raise MissingNonceError(
            f"{request.name} sent no nonce. message ignored"
        )

    if request.nonce in nonces:
        raise DuplicateNonceError(
            f"{request.name} reused a nonce. message ignored"
        )

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
        raise MissingTtlError(
            f"{request.name}´s scroll has no TTL. message ignored"
        )

    if request.expired:
        raise ExpiredTtlError(
            f"{request.name}´ scroll TTL expired. message ignored"
        )


# --------------------------------------------------------------------------------------
async def process_registration_reply(reply: dict[str, Any]) -> Scroll | None:
    """Process the registration reply, return a Scroll, if possible.

    Parameters
    ----------
    reply : dict[str, Any]
        _description_

    Returns
    -------
    tuple | None
        _description_

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
            logger.critical("invalid reply from collector: %s", reply)

        raise RegistrationError(f"registration: FAIL ({error})")


async def call_them_callbacks(actions: Sequence[Coroutine], payload: Any) -> None:
    for action in actions:
        try:
            await action(payload)
        except Exception as e:
            logger.critical("action failed: %s", action, exc_info=1)
            raise Exception() from e


async def _get_response(ctx: ContextT, scroll: Scroll, peer_scroll: Scroll) -> dict:
    """Attempts to register with a specific peer.

    Parameters
    ----------
    ctx : ContextT
        the current ZeroMQ context

    scroll : Scroll
        service description for local/calling service

    peer_scroll : Scroll
        service description for the peer

    Returns
    -------
    dict
        registration response from peer, if successful

    Raises
    ------
    ValueError
        if the provided scroll has no registration endpoint
    MaxRetriesReached
        raises after more than DEFAULT_RGSTR_MAX_ERRORS errors/timeouts
    """

    # prepare variables
    response, registered, errors, last_log = None, False, 0, time()
    public_key, private_key = generate_curve_key_pair()
    rgstr_endpoint = peer_scroll.endpoints.get("registration", None)

    if not rgstr_endpoint:
        raise ValueError(
            f"no registration endpoint found for {peer_scroll.service_name}"
        )

    # good luck!
    while not registered:
        now = time()
        log_it = now > last_log + DEFAULT_RGSTR_LOG_INTERVAL

        # exponential backoff for exceptions, not used if collector is
        # just unreachable because it could be back at any moment, and
        # ... the show must go on!
        time_out_after_error = TTL + 2 ** errors

        # NOTE: We need to consider the possibility that collctor is
        #       offline/unreachable. But we also don't want to wait
        #       forever for a response. Therefore we will use a
        #       timeout, but that means that the socket needs to be
        #       a one-time socket that is recreated for every attempt.
        #       Otherwise, requests would pile up in the local queue,
        #       and collector would receive duplicate messages (also
        #       with expired TTL).
        #       I tried different socket options, but couldn't find a
        #       better way to do this. But maybe there is one ...?!

        # send the registration request
        if log_it:
            logger.info("... sending registration request ...")
            logger.debug("...config: %s", peer_scroll)
            last_log = now

        with zmq.asyncio.Socket(ctx, zmq.DEALER) as sock:
            # configure socket & connection encryption
            sock.setsockopt(zmq.LINGER, TTL)
            sock.curve_secretkey = private_key.encode("ascii")
            sock.curve_publickey = public_key.encode("ascii")
            sock.curve_serverkey = peer_scroll.public_key.encode("ascii")
            sock.connect(rgstr_endpoint)

            # send the request
            await scroll.send(sock)

            # ... & wait for an answer, retry if necessary
            try:
                response = await asyncio.wait_for(sock.recv_json(), TTL)

            except asyncio.TimeoutError:
                # don't log it every time
                if log_it:
                    logger.warning(
                        "... collector unreachable, but I won't give up easily"
                    )
                errors += 1

            except zmq.ZMQError as e:
                logger.error(f"ZMQ error: {e} for {scroll}")
                logger.info("will try again in %s seconds...", time_out_after_error)
                await asyncio.sleep(time_out_after_error)
                errors += 1

            except Exception as e:
                logger.error(f"unexpected error: {e} for: {scroll}", ecx_info=1)
                logger.info("will try again in %s seconds...", time_out_after_error)
                await asyncio.sleep(time_out_after_error)
                errors += 1

            else:
                logger.debug(f"received reply from collector: {response}")
                registered = True

            finally:
                if not response and errors > DEFAULT_RGSTR_MAX_ERRORS:
                    raise MaxRetriesReached(
                        "unable to register with collector {} after {} errors"
                        .format(rgstr_endpoint, DEFAULT_RGSTR_MAX_ERRORS)
                    )

    return response


async def register(
    ctx: ContextT,
    config: ConfigT,
    rgstr_info_fn: Callable,
    actions: Optional[Sequence[Coroutine]] = None
) -> Scroll:
    """Register with a peer kinsman.

    Parameters
    ----------
    ctx : zmq.Context
        the current ZMQ context

    config : ConfigT
        this streamers configuration object

    rgstr_info_fn : Callable
        a function that return the registration information.

    actions : Optional[Sequence[Coroutine]], optional
        coroutines to call after successful registration, default None

    Returns
    -------
    Scroll
        registration reply

    Raises
    ------
    RegistrationError
        if the registration for all possible peers failed
    """

    # build formalized registration request
    scroll = Scroll.from_dict(config.as_dict())
    registered = False

    while not registered:

        # rgstr_info_fn may or may not try to to retrieve updated
        # registration information from the Central Service Registry.
        # This makes no sense if we are trying to initially register
        # the caller with the CSR. Then rgstr_info_fn should return
        # the static information for the Central Service Registry.
        while not (peer_scroll := await rgstr_info_fn()):
            asyncio.sleep(10)

        logger.info(
            "... registering with %s - at %s",
            peer_scroll.service_name,
            peer_scroll.endpoints.get("registration"),
        )

        try:
            response = await _get_response(ctx, scroll, peer_scroll)

        except ValueError as e:
            logger.error(e)
            await asyncio.sleep(5)

        except MaxRetriesReached as e:
            logger.error(e)

        else:
            # build formalized registration reply from dictionary response
            scroll = await process_registration_reply(response)

            # execute  provided actions with reply, if any
            if actions:
                await call_them_callbacks(actions, scroll)

            logger.info("===================================================")
            logger.info("registered with collector at %s", config.register_at)

    return scroll


async def send_reply(
    req: Scroll,
    config: ConfigT,
    kinsfolk: KinsfolkT
):
    reply = Scroll.from_dict(config.as_dict())

    await reply.send(socket=req._socket, routing_key=req._routing_key)


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
                for callback in callbacks:
                    await callback(request)

            # send to queue(s) for valid request
            if queues:
                for queue in queues:
                    queue.put_nowait(request)

            await asyncio.sleep(1)

        except zmq.ZMQError as e:
            logger.error("registration monitor -> zmq error: %s", e, exc_info=1)
            break
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
        except Exception as e:
            logger.exception("unexpected error: %s", e, exc_info=1)
            # logger.error("exception caused by this msg: %s", msg_bytes)

    logger.info("registration monitor stopped: OK")


# ======================================================================================
#                                Registration OOP style                                #
# ======================================================================================
class Rawi:
    """Class that manages registrations for peer kinsman.

    rawi --> Arabic for registry
    """

    def __init__(
        self,
        ctx: ContextT,
        config: ConfigT,
        rgstr_info_fn: Callable,
        actions: Optional[Sequence[Coroutine[Scroll, None, None]]] = None
    ) -> None:
        """Initialize the Rawi (registration) instance.

        Parameters
        ----------
        ctx : ContextT
            Currently active (async) ZeroMQ context

        config : ConfigT
            The co´mponent configuration object

        rgstr_info_fn : Callable[None, Scroll]
            A function that returns registration information. It
            should return a Scroll instance from the peer that this
            component should register with.

        actions : Optional[Sequence[Coroutine[Scroll, None, None]]], optional
            Actions to perform after a peer registered with this component.
        """
        self.ctx = ctx
        self.config = config
        self.rgstr_info_fn = rgstr_info_fn
        self.actions = actions

        # configure the registration socket
        logger.info("configuring registration socket at %s", config.rgstr_addr)
        logger.debug("public key: %s", config.public_key)

        self.rgstr_sock = ctx.socket(zmq.ROUTER)
        self.rgstr_sock.curve_secretkey = config.private_key.encode("ascii")
        self.rgstr_sock.curve_publickey = config.public_key.encode("ascii")
        self.rgstr_sock.curve_server = True
        self.rgstr_sock.bind(config.rgstr_addr)

        self.monitor_task = None

        logger.debug("Rawi instance created")

    # ..................................................................................
    async def start(self):
        """Start the Rawi instance"""
        self.monitor_task = asyncio.create_task(monitor_registration(self.rgstr_sock))

        if self.config.service_registry is not None:
            rgstr_info_fn = partial(
                self.static_rgstr_info_fn, self.config.service_registry
            )

            await self.register(rgstr_info_fn=rgstr_info_fn, actions=[])

    async def stop(self):
        """Stop the Rawi instance"""
        if self.monitor_task:
            self.monitor_task.cancel()

        self.rgstr_sock.close()

    # ..................................................................................
    async def register(
        self,
        rgstr_info_fn: Optional[Coroutine] = None,
        actions: Optional[Sequence[Coroutine]] = None
    ) -> None:

        logger.info("Registering ...")

        try:
            await register(
                ctx=self.ctx,
                config=self.config,
                rgstr_info_fn=rgstr_info_fn or self.static_rgstr_info_fn,
                actions=actions or self.actions
            )
        except RegistrationError as e:
            logger.critical(e)
        except Exception as e:
            logger.critical("unexpected error: %s", e, exc_info=1)

    async def send_reply(
        self,
        scroll: Scroll,
        config: ConfigT,
        error: Optional[str] = None,
    ):
        """Sends a reply for a registration request that we received.

        Parameters
        ----------
        scroll : Scroll
            the peer scroll from the registration request

        config: ConfigT
            our configuration

        error: Optional[str], optional
            any error that might have occured while processing the request
        """

        if not error:
            reply = Scroll.from_dict(config.as_dict())
            await reply.send(socket=scroll._socket, routing_key=scroll._routing_key)

        else:
            # hopefully this works to inform the client about the error,
            # but depending on the error, this might not be possible
            try:
                await self.rgstr_sock.send_multipart(
                    [
                        scroll._routing_key,
                        json.dumps({"error": error}).encode("utf-8")
                    ]
                )
            except AttributeError as e:
                logger.error(e)
                return
            except zmq.ZMQError as e:
                logger.error(e)
            except Exception as e:
                logger.error("unexpected error: %s", e, exc_info=1)

    async def static_rgstr_info_fn(self, service_registry: dict) -> NamedTuple:
        """Return static registration information for the Central Service Registry."""

        if not isinstance(service_registry, dict):
            raise RegistrationError("service_registry in config must be a dictionary")

        if "endpoint" not in service_registry:
            raise RegistrationError("service_registry in config, missing: 'endpoint'")

        class StaticRegistrationInfo(NamedTuple):
            service_name: str
            endpoint: str
            public_key: str
            endpoints: dict

        rgstr_endpoint = service_registry.get("endpoint", None)

        return StaticRegistrationInfo(
            service_name="Central Service Registry",
            endpoint=rgstr_endpoint,
            public_key=service_registry.get("public_key", None),
            endpoints={"registration": rgstr_endpoint}
        )
