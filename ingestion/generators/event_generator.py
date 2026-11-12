"""Synthetic order-event generator.

What it simulates
-----------------
* An order has a lifecycle: CREATED -> PAID -> SHIPPED -> DELIVERED, or
  CREATED -> CANCELLED. Every step is a separate event carrying the full order
  snapshot, so one ``order_id`` produces several events over time.
* A small share of events is deliberately wrong, so the pipeline's safeguards
  are exercised instead of just claimed:
    - invalid   : a field is missing or impossible (must be quarantined)
    - duplicate : the same event (same ``event_id``) is delivered twice
    - late      : the event is delivered minutes after it happened, possibly
                  after a newer event for the same order (out of order)

The generator keeps counters (``stats``) of everything it injected. The
pipeline's own counts can be compared against them.
"""

from __future__ import annotations

import copy
import random
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from ingestion import config
from ingestion.generators.catalog import generate_customers, generate_products

PAYMENT_METHODS = ["UPI", "CARD", "NET_BANKING", "WALLET"]

# What an order in a given status can become next.
NEXT_STATUS = {
    "CREATED": "PAID",
    "PAID": "SHIPPED",
    "SHIPPED": "DELIVERED",
}
FINAL_STATUSES = {"DELIVERED", "CANCELLED"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SimulatedClock:
    """A clock that jumps forward a fixed step every time it is read.

    Used for backfills: 5,000 events can be spread over the last two hours of
    *event time* while being generated in a second of real time.
    """

    def __init__(self, start: datetime, step: timedelta):
        self._now = start
        self._step = step

    def __call__(self) -> datetime:
        self._now += self._step
        return self._now


def _corrupt_missing_customer(event: dict) -> None:
    event["payload"]["customer_id"] = None


def _corrupt_missing_order_id(event: dict) -> None:
    event["payload"]["order_id"] = None


def _corrupt_negative_quantity(event: dict) -> None:
    event["payload"]["quantity"] = -event["payload"]["quantity"]


def _corrupt_missing_amount(event: dict) -> None:
    del event["payload"]["total_amount"]


def _corrupt_wrong_total(event: dict) -> None:
    event["payload"]["total_amount"] = round(event["payload"]["total_amount"] + 999.0, 2)


def _corrupt_bad_timestamp(event: dict) -> None:
    event["event_timestamp"] = "not-a-timestamp"


def _corrupt_unknown_status(event: dict) -> None:
    event["payload"]["order_status"] = "TELEPORTED"


CORRUPTIONS: list[Callable[[dict], None]] = [
    _corrupt_missing_customer,
    _corrupt_missing_order_id,
    _corrupt_negative_quantity,
    _corrupt_missing_amount,
    _corrupt_wrong_total,
    _corrupt_bad_timestamp,
    _corrupt_unknown_status,
]


class EventGenerator:
    """Produces order events one "tick" at a time.

    ``next_events()`` returns the events to deliver for one tick: usually one,
    sometimes two (a duplicate, or a late event whose delay has expired),
    occasionally none (the only event of the tick was held back as late).
    """

    def __init__(
        self,
        seed: int | None = None,
        run_id: str | None = None,
        clock: Callable[[], datetime] = utc_now,
        invalid_rate: float = config.INVALID_RATE,
        duplicate_rate: float = config.DUPLICATE_RATE,
        late_rate: float = config.LATE_RATE,
        late_delay: timedelta = timedelta(minutes=config.LATE_DELAY_MINUTES),
    ):
        self._rng = random.Random(seed)
        self._clock = clock
        self.invalid_rate = invalid_rate
        self.duplicate_rate = duplicate_rate
        self.late_rate = late_rate
        self.late_delay = late_delay

        # Order ids must be unique across runs, otherwise a second run would
        # overwrite the first run's orders downstream. The run id makes them so.
        self.run_id = run_id or f"{self._rng.getrandbits(32):08x}"
        self._order_number = 0

        self._customers = generate_customers()
        self._products = generate_products()

        self._open_orders: list[dict] = []  # orders that can still change status
        self._held_back: list[tuple[datetime, dict]] = []  # late events waiting to be delivered

        self.stats = {
            "events_delivered": 0,
            "orders_created": 0,
            "status_updates": 0,
            "invalid": 0,
            "duplicates": 0,
            "late": 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def next_events(self) -> list[dict]:
        now = self._clock()
        events = self._release_due_late_events(now)

        if self._open_orders and self._rng.random() < config.UPDATE_SHARE:
            event = self._advance_existing_order(now)
        else:
            event = self._create_order(now)

        if self._rng.random() < self.invalid_rate:
            self._rng.choice(CORRUPTIONS)(event)
            self.stats["invalid"] += 1

        if self._rng.random() < self.late_rate:
            # The event keeps its original event_timestamp but is delivered
            # later, which is what "late data" means.
            event["is_late_event"] = True
            self._held_back.append((now + self.late_delay, event))
            self.stats["late"] += 1
        else:
            events.append(event)
            if self._rng.random() < self.duplicate_rate:
                events.append(copy.deepcopy(event))
                self.stats["duplicates"] += 1

        self.stats["events_delivered"] += len(events)
        return events

    def flush(self) -> list[dict]:
        """Deliver every late event still held back (call once at shutdown)."""
        events = [event for _, event in self._held_back]
        self._held_back = []
        self.stats["events_delivered"] += len(events)
        return events

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _release_due_late_events(self, now: datetime) -> list[dict]:
        due = [event for release_at, event in self._held_back if release_at <= now]
        self._held_back = [(release_at, event) for release_at, event in self._held_back if release_at > now]
        return due

    def _create_order(self, now: datetime) -> dict:
        self._order_number += 1
        customer = self._rng.choice(self._customers)
        product = self._rng.choice(self._products)
        quantity = self._rng.randint(1, 5)

        order = {
            "order_id": f"O-{self.run_id}-{self._order_number:07d}",
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "brand": product["brand"],
            "quantity": quantity,
            "unit_price": product["price"],
            "total_amount": round(quantity * product["price"], 2),
            "payment_method": self._rng.choice(PAYMENT_METHODS),
            "order_status": "CREATED",
            "city": customer["city"],
            "state": customer["state"],
            "country": customer["country"],
            "order_timestamp": now.isoformat(),
        }
        self._open_orders.append(order)
        self.stats["orders_created"] += 1
        return self._envelope("ORDER_CREATED", order, now)

    def _advance_existing_order(self, now: datetime) -> dict:
        index = self._rng.randrange(len(self._open_orders))
        order = self._open_orders[index]

        if order["order_status"] == "CREATED" and self._rng.random() < config.CANCEL_SHARE:
            order["order_status"] = "CANCELLED"
        else:
            order["order_status"] = NEXT_STATUS[order["order_status"]]

        if order["order_status"] in FINAL_STATUSES:
            self._open_orders.pop(index)

        self.stats["status_updates"] += 1
        return self._envelope("ORDER_STATUS_CHANGED", order, now)

    def _envelope(self, event_type: str, order: dict, now: datetime) -> dict:
        return {
            "event_id": str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            "event_type": event_type,
            "event_version": "1.0",
            # When the thing happened in the business. Late events keep this
            # value even though they are delivered later.
            "event_timestamp": now.isoformat(),
            "generated_at": now.isoformat(),
            "is_late_event": False,
            "payload": copy.deepcopy(order),
        }
