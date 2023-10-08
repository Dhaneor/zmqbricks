#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 08 16:16:23 2023

@author_ dhaneor
"""


class BadScrollError(BaseException):
    ...


class MissingTtlError(BaseException):
    ...


class ExpiredTtlError(BaseException):
    ...


class MissingNonceError(BaseException):
    ...


class DuplicateNonceError(BaseException):
    ...


class EmptyMessageError(BaseException):
    ...


class RegistrationError(BaseException):
    pass


class MaxRetriesReached(RegistrationError):
    pass
