#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides functionality to keep track of our Kinsfolk.

Note: Wonder about kinsman/kinsfolk? According to Merriam-Webster:
    kinsman -> :RELATIVE (here: a connected peer/server/cvient)
    kinsfolk -> :KINDRED :RELATIVES (here: a bunch of them)

Classes:
    Kinsman
        a dataclass representing a kinsman

    Kinsfolk
        A registry that keeps track of connected kinsmen.

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import time

from dataclasses import dataclass
from typing import Optional, Mapping, Sequence, Coroutine

from .registration import Scroll


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)
else:
    logger = logging.getLogger("main.kinsfolk")
    logger.setLevel(logging.DEBUG)


# ======================================================================================
@dataclass
class Kinsman:
    """Represents a kinsman"""

    identity: str
    name: str
    service_name: str
    service_type: str
    endpoints: Mapping[str, str]
    exchange: str
    markets: Sequence[str]
    description: Optional[str] = None
    public_key: Optional[str] = None
    session_key: Optional[str] = None
    liveness: Optional[int] = 0
    last_seen: Optional[float] = 0
    last_health_check: Optional[int] = 0
    remark: Optional[str] = None

    def __repr__(self) -> str:
        serv_name = self.service_name.upper() if self.service_name else "UNKNOWN"

        return (
            f"Kinsman {self.name}, Keeper of the {serv_name} "
            f"({self.liveness})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __hash__(self) -> int:
        return hash(self.identity)

    @staticmethod
    def from_scroll(scroll: Scroll, initial_liveness: int) -> "Kinsman":
        """Creates a Kinsman from a Scroll message.

        Convience function to streamline registration process for
        newly conneted services.
        """

        now = time.time()

        return Kinsman(
            identity=scroll.uid,
            name=scroll.name,
            service_name=scroll.service_name,
            service_type=scroll.service_type,
            endpoints=scroll.endpoints,
            exchange=scroll.exchange,
            markets=scroll.markets,
            description=scroll.description,
            public_key=scroll.public_key,
            liveness=initial_liveness,
            last_seen=now,
            session_key=None,
            last_health_check=now,
        )


KinsfolkT = Mapping[str, Kinsman]  # type alias


class Kinsfolk:
    "A registry that keeps track of connected kinsmen."

    def __init__(self, hb_interval_seconds: int, hb_liveness: int) -> None:
        self._kinsfolk: KinsfolkT = {}

        self.hb_interval_seconds = hb_interval_seconds
        self.hb_liveness = hb_liveness

    def __contains__(self, identity: str) -> bool:
        return identity in self._kinsfolk

    def __getitem__(self, identity: str) -> Kinsman:
        return self._kinsfolk[identity]

    def __setitem__(self, identity: str, kinsman: Kinsman) -> None:
        self._kinsfolk[identity] = kinsman

    def __delitem__(self, identity: str) -> None:
        del self._kinsfolk[identity]

    def __iter__(self) -> KinsfolkT:
        return iter(self._kinsfolk)

    def __len__(self) -> int:
        return len(self._kinsfolk)

    # .................................................................................
    async def watch_out(self, actions: Optional[Sequence[Coroutine]] = None) -> None:
        """Watches over the health of the Kinsfolk"""
        logger.debug("watching out ...")

        while True:
            try:
                await asyncio.sleep(self.hb_interval_seconds)

                if self._kinsfolk:
                    self.check_health()

                # execute provide actions in case we have no more kinsman left
                if not self._kinsfolk and actions:
                    for action in actions:
                        logger.debug("executing action %s", action)
                        await action()
            except asyncio.CancelledError:
                logger.debug("watch out task cancelled ...")
                break
            except Exception as e:
                logger.error("unexpected error: %s", e, exc_info=1)

        logger.debug("watching out done")

    async def get_all(self, service_type: str) -> KinsfolkT:
        return [k for k in self._kinsfolk.values() if k.service_type == service_type]

    # .................................................................................
    async def add(self, kinsman: Kinsman) -> None:
        """Adds a new Kinsman to the Kinsfolk"""
        self._kinsfolk[kinsman.identity] = kinsman

    async def update(self, identity: str, *args) -> None:
        """Updates the health for the Kinsman with the given identity-"""
        if identity in self._kinsfolk:
            self._kinsfolk[identity].last_seen = time.time()
            self._kinsfolk[identity].liveness = self.hb_liveness
            logger.debug("'HOY!' from %s", self._kinsfolk[identity])
        else:
            logger.warning(
                "Kinsman %s not found in Kinsfolk -> %s", identity, self._kinsfolk
            )

    async def accept(self, scroll: Scroll) -> bool:
        """Accepts a new Kinsman to the Kinsfolk

        Parameters
        ----------
        scroll : Scroll
            A registration request to accept a new Kinsman to the Kinsfolk.

        Returns
        -------
        bool
            Accepted, yes or not.
        """
        if scroll.uid in self._kinsfolk:
            logger.warning(
                "Kinsman %s already in Kinsfolk -> %s",
                scroll.name,
                ", ".join([k.name for k in self._kinsfolk.values()]),
            )
            return True

        try:
            kinsman = Kinsman.from_scroll(scroll, self.hb_liveness)
            self._kinsfolk[kinsman.identity] = kinsman
            logger.info(
                "Welcome to Kinsman %s, Keeper of the %s for %s (%s)",
                kinsman.name,
                kinsman.service_type.upper(),
                kinsman.exchange.upper(),
                ", ".join(kinsman.markets),
            )
        except Exception as e:
            logger.error(f"unexpected error creating a Kinsman: {e}")
            return False
        else:
            return True

    def check_health(self) -> None:
        """Checks the health of the Kinsfolk"""

        now = time.time()

        self._kinsfolk = {
            id: kinsman
            for id, kinsman in self._kinsfolk.items()
            if self._is_alive(kinsman, now)
        }

    def _is_alive(self, kinsman: Kinsman, now: int) -> bool:
        if now - kinsman.last_seen < self.hb_interval_seconds:
            return True

        kinsman.liveness -= 1
        kinsman.last_seen = now

        logger.info(
            "kinsman %s´s health is degrading: %s", kinsman.name, kinsman.liveness
        )

        # well, he/she is still alive
        if kinsman.liveness > 0:
            return True

        logger.warning(
            "Thy shall mourn the passing of our kinsman  %s!", kinsman.name
        )

        return False
