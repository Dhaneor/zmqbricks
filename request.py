#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 22:30:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import zmq

from time import time
from typing import TypeAlias, TypeVar
from zmq.utils.monitor import parse_monitor_message

from zmqbricks.fukujou.curve import generate_curve_key_pair
from zmqbricks.exceptions import MaxRetriesReached, BadScrollError

ContextT: TypeAlias = zmq.asyncio.Context
ScrollT = TypeVar("ScrollT", bound=object)

logger = logging.getLogger("main.one_time_request")
logger.setLevel(logging.INFO)


LOG_INTERVAL = 900  # log the same message again after (secs)
MAX_ERRORS = 10  # maximum number of registration errors

TTL = 10  # time-to-live for a scroll (seconds)
DEBUG = False  # set this to True for debuggging encrypted registration attempts


async def one_time_request(
    ctx: ContextT, client: ScrollT, server: ScrollT, endpoint: str
):
    """Send a one-time request to specific peer/server.

    NOTE: We need to consider the possibility that our
    peer is offline/unreachable. But we also don't want
    to wait forever for a response. Therefore we will use
    a timeout, but that means that the socket needs to be
    a one-time socket that is recreated for every attempt.
    Otherwise, requests would pile up in the local queue,
    and the server would receive duplicate messages (also
    with expired TTL).
    I tried different socket options, but couldn't find a
    better way to do this. But maybe there is one ...?!

    Parameters
    ----------
    ctx
        the current ZeroMQ context
    client
        a class with a 'send' method that accepts a socket
        as argument and that sends appropriatly formatted message
        when called (message format depends on the server)
    server
        service description for the peer, must have attributes:
        - name
        - public_key
        - endpoint

    Returns
    -------
    dict
        registration response from peer, if successful

    Raises
    ------
    BadScrollError
        if the provided server scroll has no registration endpoint attribute
    MaxRetriesReached
        raises after MAX_ERRORS errors/timeouts
    """

    # prepare variables
    response, errors, last_log = None, 0, time() - LOG_INTERVAL
    srv_name, srv_pubkey = server.name, server.public_key

    if not (srv_endpoint := server.endpoints.get(endpoint)):
        raise BadScrollError(f"no endpoint found for {srv_name}")

    # good luck!
    while True:
        now, time_out_after_error = time(), TTL + 2**errors

        # send the registration request
        if log_it := now > last_log + LOG_INTERVAL:
            logger.info("... sending request to %s", server)

        with zmq.asyncio.Socket(ctx, zmq.DEALER) as sock:
            try:
                logger.debug("... connecting to %s", srv_endpoint)
                logger.debug("... will use server public key: %s" % srv_pubkey)
                public_key, private_key = generate_curve_key_pair()

                # configure socket & connection encryption
                sock.setsockopt(zmq.LINGER, TTL)
                sock.curve_secretkey = private_key.encode("ascii")
                sock.curve_publickey = public_key.encode("ascii")
                sock.curve_serverkey = srv_pubkey.encode("ascii")
                sock.connect(srv_endpoint)

                # send the request
                await client.send(sock)

                # activate the DEBUG flag at the top of the file to
                # see what's going on with encrypted connections
                if DEBUG:
                    monitor = sock.get_monitor_socket()

                    try:
                        event = await asyncio.wait_for(monitor.recv_multipart(), 2)
                        parsed = parse_monitor_message(event)
                        logger.debug("monitor msg: %s", parsed)
                    except asyncio.TimeoutError:
                        pass
                    except zmq.ZMQError as e:
                        logger.error(
                            "registration monitor -> zmq error: %s", e, exc_info=1
                        )

                # ... & wait for an answer, retry if necessary
                response = await asyncio.wait_for(sock.recv_multipart(), TTL)

            except ConnectionRefusedError as e:
                logger.error("connection refused: %s", e, exc_info=1)

            except asyncio.TimeoutError:
                # don't log it every time
                if log_it:
                    logger.warning("... %s is quiet, but I won't give up", srv_name)
                errors += 1

            except zmq.ZMQError as e:
                logger.error(f"ZMQ error: {e} for {client}")
                logger.info("will try again in %s seconds...", time_out_after_error)
                await asyncio.sleep(time_out_after_error)
                errors += 1

            except Exception as e:
                logger.error(f"unexpected error: {e} for: {client}", ecx_info=1)
                logger.info("will try again in %s seconds...", time_out_after_error)
                await asyncio.sleep(time_out_after_error)
                errors += 1

            else:
                logger.debug("... rcv reply from %s: %s", srv_name, response)
                return response

            finally:
                if not response and errors > MAX_ERRORS:
                    raise MaxRetriesReached(
                        "... unable to reach {}r {} after {} errors"
                        .format(srv_name, srv_endpoint, MAX_ERRORS)
                    )
                last_log = now
