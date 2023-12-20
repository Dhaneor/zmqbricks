#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a general configuration object/class for ZMQ components.

This is intended to be used as a base class for specific (inherited)
configuration classes for each component.

Shared values (e.g. heartbeat interval, liveness, etc. ...) are defined
in a separate file that must be available in this directory.

Created on Sat Oct 07 13:28:23 2023

@author_ dhaneor
"""
import json
import requests
from typing import Sequence, Optional, TypeVar
from uuid import uuid4

try:
    from . import config as cnf
    from .fukujou.curve import generate_curve_key_pair
    from .util.sockets import SockDef
except ImportError:
    import config as cnf
    from .fukujou.curve import generate_curve_key_pair
    from .util.sockets import SockDef


class BaseConfig:
    """Base configuration for components."""

    name: str = "you really should set a name"
    service_type: str = "generic"  # type of service, e.g. "streamer", ...
    desc: str = ""  # service description, just for printing, not essential

    encrypt: bool = True  # use encryption or not

    hb_interval: float = cnf.HB_INTERVAL  # heartbeat interval (seconds)
    hb_liveness: int = cnf.HB_LIVENESS  # heartbeat liveness (max missed heartbeats)

    rgstr_with_csr: bool = True
    rgstr_timeout: int = cnf.RGSTR_TIMEOUT  # registration timeout (seconds)
    rgstr_max_errors: int = cnf.RGSTR_MAX_ERRORS  # max no of registration errors
    rgstr_log_interval: int = cnf.RGSTR_LOG_INTERVAL  # resend request after (secs)

    dev_env: bool = cnf.DEV_ENV  # development environment or not

    def __init__(
        self,
        exchange: Optional[str] = "all",
        markets: Optional[Sequence[str]] = ["all"],
        service_type: Optional[str] = "generic",
        sock_defs: Sequence[SockDef] = None,
        **kwargs
    ) -> None:
        self.uid: str = str(uuid4())

        self.name = kwargs.get("name", BaseConfig.name)
        self.service_type = service_type or BaseConfig.service_type

        self.exchange: str = exchange
        self.markets: Sequence[str] = markets
        self.desc: Optional[str] = kwargs.get("desc", BaseConfig.desc)

        self._sock_defs: Sequence[SockDef] = sock_defs
        self._hb_interval: float = kwargs.get("hb_interval", BaseConfig.hb_interval)
        self._hb_liveness: int = kwargs.get("hb_liveness", BaseConfig.hb_liveness)

        self.service_registry: Optional[dict[str, str]] = None
        self._rgstr_timeout: int = kwargs.get(
            "rgstr_timeout", BaseConfig.rgstr_timeout
        )
        self._rgstr_max_errors: int = kwargs.get(
            "rgstr_max_errors", BaseConfig.rgstr_max_errors
        )

        self.public_key, self.private_key = generate_curve_key_pair()

        self._endpoints: dict[str, str] = {}

    @property
    def service_name(self) -> str:
        return (
            f"{self.service_type.capitalize()} for {self.exchange.upper()} "
            f"{[m.upper() for m in self.markets]}"
        )

    @property
    def endpoints(self) -> dict[str, str]:
        if not cnf.DEV_ENV:
            ip = self.external_ip()

            for name, endpoint in self._endpoints.items():
                self._endpoints[name] = endpoint.replace("*", ip)
                self._endpoints[name] = endpoint.replace("127.0.0.1", ip)
                self._endpoints[name] = endpoint.replace("localhost", ip)

        return self._endpoints

    @property
    def hb_addr(self) -> str:
        return self.endpoints.get("heartbeat", None)

    @property
    def rgstr_addr(self) -> str:
        return self.endpoints.get("registration", None)

    @property
    def req_addr(self) -> str:
        return self.endpoints.get("requests", None)

    @property
    def pub_addr(self) -> str:
        return self.endpoints.get("publisher", None)

    @property
    def mgmt_addr(self) -> str:
        return self.endpoints.get("management", None)

    @property
    def external_ip(self) -> str:
        return requests.get('https://api.ipify.org').text

    # ..................................................................................
    def as_dict(self) -> dict:
        """Get the dictionary representation"""
        return {
            "service_type": self.service_type,
            "service_name": self.service_name,
            "endpoints": self.endpoints
        } | {
            var: getattr(self, var) for var in vars(self)
            if not var.startswith("_") or var == "private_key"
        }

    def as_json(self) -> str:
        """Get the JSON representation"""
        return json.dumps(self.as_dict(), indent=2)

    # ..................................................................................
    @staticmethod
    def from_json(json_str: str) -> "BaseConfig":
        """Build a configuration object from a JSON string."""
        return json.loads(json_str, object_hook=BaseConfig.from_dict)

    @staticmethod
    def from_dict(d: dict) -> "BaseConfig":
        """Build a configuration object from a dictionary."""
        return BaseConfig(**d)


ConfigT = TypeVar("ConfigT", bound=BaseConfig)
