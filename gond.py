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
import zmq
import zmq.asyncio

from typing import TypeVar, Optional

from . import kinsfolk as kf
from . import registration as rgstr
from .import heartbeat as hb
from . import base_config as cnf

configT = TypeVar("configT", bound=cnf.BaseConfig)
ContextT = TypeVar("ContextT", bound=zmq.asyncio.Context)


# ======================================================================================
class Gond:
    """Skeleton class for components in the data sources/analysis framework."""

    kinsfolk: kf.Kinsfolk  # kinsfolk registry component
    registration: rgstr.Rawi  # registration monitor component
    heartbeat: hb.Hjarta  # heartbeat sending/monitoring component
    craeft: object  # craeft component (the main task of the component)

    def __init__(self, config: configT, ctx: Optional[ContextT]) -> None:
        self.config = config
        self.ctx = ctx or zmq.asyncio.Context()

        self.tasks: list = []

    async def __aenter__(self):
        self.tasks.append(asyncio.create_task(self.heartbeat(self.ctx, self.config)))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for task in self.tasks:
            task.cancel()

        asyncio.gather(*self.tasks, return_exceptions=True)

    def run(self) -> None:
        ...
