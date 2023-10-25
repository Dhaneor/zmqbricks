#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration file basic settings for ZeroMQ components.

This is the place where shared settings can be set.

Created on Fri Oct 06 21:41:23 2023

@author_ dhaneor
"""
DEV_ENV = True


# some default values that are (can) be shared between components
ENCODING = "utf-8"
HB_INTERVAL = 3  # seconds
HB_LIVENESS = 10  # heartbeat liveness
RGSTR_TIMEOUT = 15  # seconds
RGSTR_LOG_INTERVAL = 900  # re-log registration errors after (secs)
RGSTR_MAX_ERRORS = 10  # maximum number of registration errors
