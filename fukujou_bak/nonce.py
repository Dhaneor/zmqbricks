#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 04 13:55:23 2023

@author_ dhaneor
"""
import time
import os
import hashlib


class Nonce:

    validity_period: int = 300  # validity period in seconds

    def __init__(self):
        self.nonce_store = []

    @classmethod
    def get_nonce(length: int = 32):
        # Current system time in milliseconds
        now = int(time.time() * 1000)

        # Generate <length> bytes of random data
        random_data = os.urandom(32)

        # Combine them
        combined_data = f"{now}{random_data.hex()}".encode('utf-8')

        # Hash using SHA256
        return hashlib.sha256(combined_data).hexdigest()

    def is_valid_nonce(self, nonce) -> bool:
        now = time.time()

        # purge expired nonces
        self.nonce_store = set(
            elem for elem in self.nonce_store if now - elem[1] < self.validity_period
        )

        # # check if nonce is already used
        if nonce in set((record[0] for record in self.nonce_store)):
            return False

        # store the new nonce with the current timestamp
        self.nonce_store.add((nonce, now))

        return True


if __name__ == "__main__":
    n = Nonce()

    for i in range(10_000):
        nonce = Nonce.get_nonce()
        if i % 2 == 0:
            assert ~(n.is_valid_nonce(nonce)), \
                f"should be False: {str(n.is_valid_nonce(nonce))}"
