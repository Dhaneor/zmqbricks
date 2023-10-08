#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a class that simplifies the creation of components for
the data sources/analysis framework.

The Gond class combines the following parts:
- kinsfolk registry
- heartbeat sending/monitoring
- registration of new kinsmen (peers)

Created on Sat Oct 07 12:01:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import zmq
import zmq.asyncio

from typing import TypeVar, Optional

from . import kinsfolk as kf
from . import registration as rgstr
from . import heartbeat as hb
from . import base_config as cnf
from . import exceptions as exc

logger = logging.getLogger("main.gond")

configT = TypeVar("configT", bound=cnf.BaseConfig)
ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)


# ======================================================================================
class Gond:
    """Skeleton class for components in the data sources/analysis framework.

    This is to be used as a context manager!

    In the world of Middle-earth, an elven fortified settlement is
    called a "gond", which can be translated as a "stronghold" or
    "keep". The word itself comes from the Elvish language Sindarin,
    where it means "stronghold of stone". The term is also used to
    refer to the capital city of Gondor, which is fitting given the
    city's ancient and impenetrable architecture.
    """

    kinsfolk = kf.Kinsfolk  # kinsfolk registry component
    registration = rgstr.Rawi  # registration monitor component
    heartbeat = hb.Hjarta  # heartbeat sending/monitoring component
    craeft = None  # craeft component (the main task of the component)

    rgstr_info_fn = None  # a function that returns registration information

    def __init__(self, config: configT, ctx: Optional[ContextT] = None, **kwargs) -> None:
        self.config = config
        self.ctx = ctx or zmq.asyncio.Context()

        self.kinsfolk = kf.Kinsfolk(config.hb_interval, config.hb_liveness)
        self.heart = self.heartbeat(self.ctx, self.config)
        self.rawi = self.registration(self.ctx, self.config, lambda x: {})

        self.tasks: list = []

    def __repr__(self) -> str:
        return f"Gond(config={vars(self.config)})"

    async def __aenter__(self):
        logger.info("Starting components...")

        # heartbeats ...
        self.tasks.append(asyncio.create_task(self.heart.start()))

        # registration monitoring ...
        self.tasks.append(asyncio.create_task(self.rawi.start()))

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.info("Stopping components...")

        await self.rawi.stop()
        await self.heart.stop()

        for task in self.tasks:
            task.cancel()

        asyncio.gather(*self.tasks, return_exceptions=True)

    async def process_registration(self, scroll: rgstr.Scroll) -> None:
        """Process a new registration.

        Parameters
        ----------
        scroll : rgstr.Scroll
            Scroll instance containing the peer information.
        """
        try:
            if await self.kinsfolk.accept(scroll):
                await self.heart.listen_to(scroll.endpoints.get("heartbeat", None))
            error = None
        except exc.BadScrollError as e:
            logger.error("bad scroll: %s -> %s", scroll, e)
            error = "bad scroll"
        except KeyError as e:
            logger.error("scroll gave us no valid endpoint: %s -> %s", scroll, e)
            error = "bad heartbeat endpoint"
        except zmq.ZMQError as e:
            logger.error("ZMQ error while connecting to endpoint: %s", e)
        except Exception as e:
            logger.error("unexpected error: %s", e, exc_info=1)
            error = f"unexpected error: {e}"
        finally:
            await self.rawi.send_reply(scroll=scroll, config=self.config, error=error)
