#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides functionality an asynchronous timer with reset functionality.

Created on Sun Sep 24 22:34:23 2023

@author_ dhaneor
"""
import asyncio

from random import random
from typing import Coroutine


def create_timer(
    period: float,
    callback: Coroutine[None, None, None],
    randomize: bool = False
) -> tuple[Coroutine[None, None, None], Coroutine[None, None, None]]:
    """Creates an asynchronous timer with reset.

    Parameters
    ----------
    period : float
        Time after which the callback will be executed & the timer reset.

    callback : Coroutine[None, None, None]
        Callback coroutine to be executed.

    random : bool, optional
        Add slight variation to period, yes/no, defaults to False

    Returns
    -------
    tuple[Coroutine[None, None, None], Coroutine[None, None, None]]
        * The timer coroutine.
        * The reset timer coroutine.
    """
    last_triggered = 0

    async def reset_timer():
        nonlocal last_triggered
        now = asyncio.get_running_loop().time()
        variation = period * (random() - 0.5) / 4 if randomize else 0
        last_triggered = now + variation

    async def timer():
        nonlocal last_triggered

        while True:
            try:
                now = asyncio.get_running_loop().time()

                if now - last_triggered >= period:
                    await callback()
                    await reset_timer()

                await asyncio.sleep(period)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(e)

    return timer, reset_timer
