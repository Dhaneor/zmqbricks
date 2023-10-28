#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides functionality to keep track of our Kinsfolk.

Note: Wonder about kinsman/kinsfolk? According to Merriam-Webster:
    kinsman -> :RELATIVE (here: a connected peer/server/cvient)
    kinsfolk -> :KINDRED :RELATIVES (here: a bunch of them)

Classes:
    Kinsman
        A dataclass representing a kinsman

    Kinsfolk
        A registry that keeps track of connected kinsmen.

Created on Tue Sep 12 19:41:23 2023

@author_ dhaneor
"""
import asyncio
import logging
import time

from dataclasses import dataclass
from typing import Optional, Mapping, Sequence, Coroutine, Any, TypeVar

from .registration import Scroll, ScrollT
from .exceptions import BadScrollError

logger = logging.getLogger("main.kinsfolk")
logger.setLevel(logging.INFO)

CallbacksT = TypeVar("CallbacksT", bound=Sequence[Coroutine[Any, Any, None]])

GRACE_PERIOD = 86400  # delete inactive kinsman after x seconds


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
    certificate: Optional[str] = None
    status: str = "active"
    liveness: Optional[int] = 0
    last_seen: Optional[float] = 0
    last_health_check: Optional[int] = 0
    remark: Optional[str] = None

    def __repr__(self) -> str:
        serv_name = self.service_name.upper() if self.service_name else "UNKNOWN"

        return (
            f"Kinsman {self.name}, Keeper of the {serv_name} "
            f"for {self.exchange} ({self.liveness})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __hash__(self) -> int:
        return hash(self.identity)

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    def activate(self) -> None:
        self.status = "active"

    def deactivate(self) -> None:
        self.status = "inactive"

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
            session_key=scroll.session_key,
            certificate=scroll.certificate,
            liveness=initial_liveness,
            last_seen=now,
            last_health_check=now,
        )

    def to_scroll(self) -> Scroll:
        """Get a Scroll representation of this Kinsman."""
        return Scroll(
            uid=self.identity,
            name=self.name,
            service_name=self.service_name,
            service_type=self.service_type,
            endpoints=self.endpoints,
            exchange=self.exchange,
            markets=self.markets,
            description=self.description,
            public_key=self.public_key,
            session_key=self.session_key,
            certificate=self.certificate,
        )


KinsmanT = TypeVar("KinsmanT", bound=Kinsman)


class Kinsfolk:
    "A registry that keeps track of connected kinsmen."

    def __init__(
        self,
        hb_interval_seconds: int,
        hb_liveness: int,
        on_inactive_kinsman: Optional[Sequence[Coroutine]] = None,
        grace_period: int = 86400,
    ) -> None:
        """Initialize the Kinsfolk.

        Parameters
        ----------
        hb_interval_seconds : int
            heartbeat interval in seconds (for health checks).
        hb_liveness : int
            initial liveness value (for health checks).
        on_inactive_kinsman : Optional[Sequence[Coroutine]], optional
            what to call when an inactive kinsman is detected, by default None.
        grace_period : int, optional
            delete inactive kinsman after this period, by default 86400
        """
        self._kinsfolk: dict[str, KinsmanT] = {}

        self.hb_interval_seconds = hb_interval_seconds
        self.hb_liveness = hb_liveness
        self.grace_period = grace_period
        self.on_inactive_kinsman = on_inactive_kinsman or []

    def __contains__(self, identity: str) -> bool:
        return identity in self._kinsfolk

    def __getitem__(self, identity: str) -> Kinsman:
        return self._kinsfolk[identity]

    def __setitem__(self, identity: str, kinsman: Kinsman) -> None:
        self._kinsfolk[identity] = kinsman

    def __delitem__(self, identity: str) -> None:
        del self._kinsfolk[identity]

    def __iter__(self) -> "Kinsfolk":
        return iter(self._kinsfolk)

    def __len__(self) -> int:
        return len(self._kinsfolk)

    @property
    def active_kinsmen(self) -> "Kinsfolk":
        return {
            identity: kinsman
            for identity, kinsman in self._kinsfolk.items()
            if kinsman.is_active
        }

    # .................................................................................
    async def watch_out(self, actions: CallbacksT = None) -> None:
        """Watch over the health of the Kinsfolk.

        NOTE: This is a blocking call and should be run as a task!

        This coroutine will watch over the health of the Kinsfolk,
        roughly after every heartbeat interval (instance attribute).
        If we did not receive a heartbeat, the 'liveness' attribute will
        be decreased by 1. When it reches 0, the kinsman´s status will
        be set to 'inactive'. Inactive kinsmen will be deleted from the
        Kinsfolk after the grace period (also instance attribute,
        defaults to 24h).

        Parameters
        ----------
        actions: Sequence[Coroutine], optional
            actions to be executed in case we have no more kinsman left
        """
        logger.info("watching out ...")

        while True:
            try:
                await asyncio.sleep(self.hb_interval_seconds)

                if self._kinsfolk:
                    self.check_health()

                # execute provided actions in case we have no more kinsman left
                if not self.active_kinsmen and actions:
                    for action in actions:
                        logger.debug("executing action %s", action)
                        await action()

            except asyncio.CancelledError:
                logger.debug("watch out task cancelled ...")
                break
            except Exception as e:
                logger.error("unexpected error: %s", e, exc_info=1)

        logger.debug("watching out done")

    async def get_all(self, service_type: str) -> list[KinsmanT]:
        return [
            k for k in self.active_kinsmen.values() if k.service_type == service_type
        ]

    # .................................................................................
    async def add(self, kinsman: Kinsman) -> None:
        """Adds a new Kinsman to the Kinsfolk"""
        self._kinsfolk[kinsman.identity] = kinsman
        logger.info("Welcome to Kinsman %s" % kinsman)

    async def update(
        self, identity: str, payload: dict = None, on_missing: Coroutine = None
    ) -> None:
        """Updates the health for the Kinsman with the given identity.

        Parameters
        ----------
        identity: str
            identity of the kinsman to update

        payload: dict
            just here for compatibility with the sender ...

        on_missing: Coroutine
            call this if the identity is unknown, should expect to receive
            the identity as an argument
        """
        kinsman = self._kinsfolk.get(identity, None)
        now = time.time()

        if kinsman:
            kinsman.last_seen, kinsman.liveness = now, self.hb_liveness
            kinsman.last_health_check = now
            logger.debug("'HOY!' from %s", self._kinsfolk[identity])
            # if the kinsman's status is set to "inactive", re-activate it
            if not kinsman.is_active:
                logger.info("... re-activating inactive kinsman")
                kinsman.activate()
        else:
            logger.warning(
                "Kinsman %s not found in Kinsfolk -> %s", identity, self._kinsfolk
            )
            if on_missing:
                await on_missing(identity)

    async def accept(self, scroll: ScrollT, on_success: CallbacksT = None) -> bool:
        """Accepts a new Kinsman to the Kinsfolk

        Parameters
        ----------
        scroll : Scroll
            A registration request to accept a new Kinsman to the Kinsfolk.
        on_success : Sequence[Coroutine], optional
            Actions to perform in case the Kinsman was accepted, by default None

        Returns
        -------
        bool
            Accepted, yes or not.

        Raises
        ------
        BadScrollError
            if the scroll is not valid / could not be processed
        """
        try:
            kinsman = Kinsman.from_scroll(scroll, self.hb_liveness)
        except Exception as e:
            logger.error(f"unexpected error creating a Kinsman: {e}")
            raise BadScrollError() from e
        else:
            await self.add(kinsman)

        for coro in on_success or []:
            try:
                await coro(scroll)
            except Exception as e:
                logger.error("callback  error in %s -->  %s", coro.__name__, e)

        return True

    def check_health(self) -> None:
        """Checks the health of the Kinsfolk"""

        now = time.time()

        self._kinsfolk = {
            identity: kinsman
            for identity, kinsman in self._kinsfolk.items()
            if (self._is_alive(kinsman, now)) | (not kinsman.is_active)
        }

    def _is_alive(self, kinsman: Kinsman, now: int) -> bool:
        # well, he/she is alive, healthy, and sent a hb message in timely manner
        if kinsman.is_active and (now - kinsman.last_seen < self.hb_interval_seconds):
            return True

        # don't consider him to be dead when his status is "inactive",
        # but do so, if the time he hasn't been seen exceeds the grace period
        if not kinsman.is_active:
            return True if now - kinsman.last_seen < self.grace_period else False

        # well, he/she is alive, but did not send a heartbeat fast enough
        kinsman.liveness -= 1
        kinsman.last_seen = now

        logger.debug(
            "kinsman %s´s health is degrading: %s", kinsman.name, kinsman.liveness
        )

        # well, he/she is still alive
        if kinsman.liveness > 0:
            return True

        # if we get here, the kinsman is at least "inactive"
        logger.warning("Thy may mourn the passing of our kinsman  %s!", kinsman.name)

        kinsman.status = "inactive"

        if self.on_inactive_kinsman:
            for coro in self.on_inactive_kinsman:
                asyncio.create_task(coro(kinsman.to_scroll()))

        return False


KinsfolkT = TypeVar("KinsfolkT", bound=Kinsfolk)
