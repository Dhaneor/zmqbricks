#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a class that simplifies the creation of components for
the data sources/analysis framework.

Using this context manager makes it possible to focus on the core
functionality for components that use it, while making sure that
the 'basic stuff' is done reliably and in a standardized and
predictable manner.

In the world of Middle-earth, an elven fortified settlement is
called a "gond", which can be translated as a "stronghold" or
"keep". The word itself comes from the Elvish language Sindarin,
where it means "stronghold of stone". The term is also used to
refer to the capital city of Gondor, which is fitting given the
city's ancient and impenetrable architecture.

The Gond class combines the following parts:
- kinsfolk registry
- heartbeat sending/monitoring
- registration of new kinsmen (peers) or with other kinsmen

Created on Sat Oct 07 12:01:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import zmq
import zmq.asyncio

from asyncio import create_task
from functools import partial
from typing import TypeVar, NamedTuple, Coroutine, Optional

from . import kinsfolk as kf
from . import registration as rgstr
from . import heartbeat as hb
from . import base_config as cnf
from .util.sockets import get_random_server_socket

logger = logging.getLogger("main.gond")

ConfigT = TypeVar("ConfigT", bound=cnf.BaseConfig)
ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)


def exc_handler(event, mystery):
    logger.critical("Unhandled exception: %s - %s", event, mystery, exc_info=1)


# ======================================================================================
class Gond:
    """Skeleton class for components in the data sources/analysis framework.

    This is to be used as a context manager!
    """

    def __init__(
        self,
        config: ConfigT,
        on_rgstr_success: Optional[list[Coroutine]] = None
    ) -> None:
        """Initialize the Gond class.

        Parameters
        ----------
        config : ConfigT
            A configuration object
        on_rgstr_success : Optional[list[Coroutine]], optional
           Additional actions to perform after successful registration
           of a peer component, by default None
        """
        self.config = config
        self.on_rgstr_success = on_rgstr_success or []

        self.kinsfolk: kf.Kinsfolk
        self.vigilante: rgstr.Vigilante
        self.heart: hb.Hjarta

        self.tasks: list = []

    def __repr__(self) -> str:
        return f"Gond(config={vars(self.config)})"

    async def __aenter__(self):
        logger.info("Starting components...")

        # set exception handler for unhandled exceptions
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(exc_handler)

        await self.initialize_sockets()
        await self.initialize_components()
        await self.create_background_tasks()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.info("Stopping components...")

        await self.vigilante.stop()
        await self.heart.stop()

        for task in self.tasks:
            logger.debug("cancelling task: %s" % task.get_name())
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def initialize_sockets(self) -> None:
        """Create the sockets for the components."""
        # CSR has a static address/ports - so, don't change it here
        if self.config.service_type == "Central Configuration Service":
            return

        # this creates the sockets & updates the config with new values
        # for their endpoints
        await get_random_server_socket("registration", zmq.ROUTER, self.config)
        await get_random_server_socket("heartbeat", zmq.PUB, self.config)

    async def initialize_components(self) -> None:
        """Initialize Kinsfolk, Vigilante, and Heartbeat components."""
        ctx = zmq.asyncio.Context.instance()

        # initialize peer registry & heartbeat classs
        self.kinsfolk = kf.Kinsfolk(self.config.hb_interval, self.config.hb_liveness)
        self.heart = hb.Hjarta(ctx, self.config, on_rcv=[self.kinsfolk.update])

        # prepare actions to perform after successful registration
        send_rgstr_reply = partial(rgstr.send_reply_ok, config=self.config)
        on_rgstr_success = [send_rgstr_reply, self.heart.add_hb_sender]
        on_rgstr_success.extend(self.on_rgstr_success)

        # initialize registration class
        on_rcv = partial(self.kinsfolk.accept, on_success=on_rgstr_success)
        self.vigilante = rgstr.Vigilante(
            config=self.config, bootstrap=self.rgstr_bootstrap, on_rcv=on_rcv
        )

        # set actions to perform if we stop receiving heartbeats
        # from a connected peer
        on_inactive = [self.heart.remove_hb_sender, self.vigilante.remove_service]
        self.kinsfolk.on_inactive_kinsman = on_inactive

    async def create_background_tasks(self) -> None:
        """Start heartbeat, registration & kinsfolk background tasks."""
        self.tasks = [
            create_task(self.heart.start(), name="heartbeats"),
            create_task(self.vigilante.start(), name="registration"),
            create_task(self.kinsfolk.watch_out(), name="kinsfolk"),
        ]

    async def rgstr_bootstrap(self) -> NamedTuple:
        """Return static registration information for the Central Service Registry.

        Takes the information about the central service registry
        from the provided configuration and returns a NamedTuple
        tht replaces the Scroll class that would normally be used
        for registrations.

        Returns
        -------
        NamedTuple
            the information needed to register with the CSR
        """

        class StaticRegistrationInfo(NamedTuple):
            name: str
            service_name: str
            endpoints: dict
            public_key: str

        rgstr_endpoint = self.config.service_registry.get("endpoint", None)

        return StaticRegistrationInfo(
            name="Amanya",
            service_name="Central Service Registry",
            endpoints={"registration": rgstr_endpoint},
            public_key=self.config.service_registry.get("public_key", None),
        )
