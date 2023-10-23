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
from typing import TypeVar

from . import kinsfolk as kf
from . import registration as rgstr
from . import heartbeat as hb
from . import base_config as cnf

logger = logging.getLogger("main.gond")

ConfigT = TypeVar("ConfigT", bound=cnf.BaseConfig)
ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)


async def exc_handler(event):
    logger.critical("Unhandled exception: %s", event, exc_info=1)


# ======================================================================================
class Gond:
    """Skeleton class for components in the data sources/analysis framework.

    This is to be used as a context manager!
    """

    def __init__(self, config: ConfigT, ctx: ContextT, **kwargs) -> None:
        self.tasks: list = []

        self.kinsfolk = kf.Kinsfolk(config.hb_interval, config.hb_liveness)
        self.heart = hb.Hjarta(ctx, config, on_rcv=[self.kinsfolk.update])
        self.kinsfolk.on_inactive_kinsman = self.heart.remove_hb_sender

        kf_accept_coro = partial(
            self.kinsfolk.accept,
            on_success=self.heart.add_hb_sender
        )

        process_rgstr_coro = partial(
            rgstr.process_registration,
            config=config,
            on_rcv=kf_accept_coro
        )

        self.rawi = rgstr.Rawi(ctx, config, None, on_rcv=process_rgstr_coro)

    def __repr__(self) -> str:
        return f"Gond(config={vars(self.config)})"

    async def __aenter__(self):
        logger.info("Starting components...")

        loop = asyncio.get_event_loop()
        loop.set_exception_handler(exc_handler)

        # start heartbeat & registration background tasks
        self.tasks = [
            create_task(self.heart.start(), name="heartbeats"),
            create_task(self.rawi.start(), name="registration"),
            create_task(self.kinsfolk.watch_out(), name="kinsfolk")
        ]

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.info("Stopping components...")

        await self.rawi.stop()
        await self.heart.stop()

        for task in self.tasks:
            logger.debug("cancelling task: %s" % task.get_name())
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)
