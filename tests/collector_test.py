#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a test client for collector.

Collector must be up and running before starting the client.
The addresses and ports configured belwo must be identical
to the actual addresses and ports.

Created on Sep 10 20:15:20 2023

@author dhaneor
"""
import json
import time
import zmq

from random import choice, random
from uuid import uuid4

hb_interval = 2
HB_ADDR = "tcp://localhost:5580"
PUBLISHER_ADDR = "tcp://localhost:5583"

topics = (
    "BTC-USDT_1min", "ETH-USDT_1min", "XRP-USDT_1min",
    "ADA-USDT_1min", "LTC-USDT_1min", "BCH-USDT_1min",
    "EOS-USDT_1min", "TRX-USDT_1min", "BNB-USDT_1min",
    "UNIX-USDT_1min", "XDC-USDT_1min", "XMR-USDT_1min",
    "QNT-USDT_1min", "DASH-USDT_1min", "ETC-USDT_1min",
    "EOS-BTC_1min", "TRX-BTC_1min", "BNB-BTC_1min",
    "ETH-BTC_1min", "XRP-BTC_1min", "ADA-BTC_1min",
)

actions = ('subscribe', 'subscribe', 'unsubscribe')


class NoTokenException(BaseException):
    pass


def main(fail=False):
    """A test client for collector.

    Runs a client for collector that sends heartbeats and
    subcribes/unsubscribes to or from a random topic
    """
    print("running main ...")
    context = zmq.Context()

    print("connecting to heartbeat socket {HB_ADDR}...")

    heartbeat = context.socket(zmq.REQ)
    heartbeat.connect(HB_ADDR)

    print(f"connecting to publisher socket {PUBLISHER_ADDR}...")
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(PUBLISHER_ADDR)

    poller = zmq.Poller()
    poller.register(heartbeat, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    counter, hb_sequence, token = 0, 0, None

    subscribed_topics = set()
    lost_connection = False

    uid = str(uuid4())
    generation = choice(('I', 'II', 'III', 'IV', 'V', 'VI', 'VII'))
    name = f"William Henry {generation}"

    next_heartbeat = time.time()
    next_sub_event = time.time()

    while True:
        try:
            events = dict(poller.poll(timeout=100))

            if heartbeat in events:
                msg = heartbeat.recv()
                msg = msg.decode()

                if ":" in msg:
                    msg, maybe_token = msg.split(":", 1)
                    token = maybe_token if not token else token

                print(f"[{counter}] {msg} <-- heartbeat")

            if subscriber in events:
                print("update received: OK")
                msg = subscriber.recv_multipart()

                event = json.loads(msg[1].decode())

                print(f"[{counter}] {event}")

            if time.time() > next_heartbeat:
                print(
                    f"[{counter}] {uid} as {name.upper()} sending "
                    f"heartbeat message to {HB_ADDR} ..."
                )

                success = False

                while not success:
                    try:
                        heartbeat.send_multipart(
                            [
                                uid.encode('utf-8'),
                                name.encode('utf-8'),
                                "hallo".encode('utf-8'),
                                "container_registry".encode('utf-8'),
                                str(hb_sequence).encode('utf-8'),
                                str(int(time.time() * 1000)).encode('utf-8'),
                            ]
                        )
                    except zmq.ZMQError as e:
                        print(f"[{counter}] collector unreachable ({e})")
                        heartbeat.close(1)
                        heartbeat = context.socket(zmq.REQ)
                        heartbeat.connect(HB_ADDR)
                        poller.register(heartbeat, zmq.POLLIN)
                        lost_connection = True
                        token = None
                        time.sleep(2)
                    else:
                        success = True

                print(f"[{counter}] hallo --> heartbeat")

                next_heartbeat = time.time() + hb_interval * 0.95

                if fail:
                    raise RuntimeError(f"[{counter}]")

            # ..........................................................................
            if time.time() > next_sub_event and token:

                if not token:
                    raise NoTokenException("waiting for token")

                if lost_connection:
                    print(f"re-subscribing to topics: {subscribed_topics}")
                    for topic in subscribed_topics:
                        sub_req = f"{topic}:{token}"
                        subscriber.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
                    lost_connection = False

                action = choice(actions) \
                    if len(list(subscribed_topics)) > 3 else "subscribe"

                next_sub_event = time.time() + 5 + random() * 10

                if action == "subscribe":
                    try:
                        topic = choice(
                            [t for t in tuple(topics) if t not in subscribed_topics]
                        )
                    except IndexError as e:
                        print(f"[{counter}] error selecting subscribe topic  {e}")
                        continue

                    sub_req = f"{topic}:{token}"
                    subscriber.setsockopt(zmq.SUBSCRIBE, sub_req.encode("utf-8"))
                    subscribed_topics.add(topic)
                    print(f"[{counter}] subscribed to topic {topic} sent: OK")
                    print(f"[{counter}] current topics: {subscribed_topics}")

                elif action == "unsubscribe" and token:
                    try:
                        topic = choice(list(subscribed_topics))
                    except IndexError as e:
                        print(f"[{counter}] error selecting unsubscribe topic {e}")
                        continue

                    sub_req = f"{topic}:{token}"
                    subscriber.setsockopt(zmq.UNSUBSCRIBE, sub_req.encode("utf-8"))
                    subscribed_topics.remove(topic)
                    print(f"[{counter}] unsubscribed from topic {topic} sent: OK")
                    print(f"[{counter}] current topics: {subscribed_topics}")

            counter += 1

        except NoTokenException as e:
            print(f"[{counter}] {e}")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[{counter}] {e}")
            break

    if fail:
        raise Exception()

    print("shutting down...")
    heartbeat.close(1)
    subscriber.close(1)
    context.term()


if __name__ == "__main__":
    main()
