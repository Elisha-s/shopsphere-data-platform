"""Command-line entry point for generating order events.

Examples
--------
Backfill 5,000 events spread over the last two hours into the landing folder
(no Kafka needed), then exit:

    python -m ingestion.run_producer --target files --max-events 5000 --backfill-minutes 120

Stream live events into the local Kafka broker, five per second, until Ctrl+C:

    python -m ingestion.run_producer --target kafka --rate 5

Stream into Azure Event Hubs (needs the variables in .env.example):

    python -m ingestion.run_producer --target eventhubs --rate 5
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import timedelta

from ingestion import config
from ingestion.generators.event_generator import EventGenerator, SimulatedClock, utc_now

logger = logging.getLogger("shopsphere.producer")


def build_producer(args: argparse.Namespace):
    # Imported lazily so the "files" target works without Kafka libraries
    # being able to reach a broker, and vice versa.
    if args.target == "files":
        from ingestion.producers.file_producer import FileEventProducer

        return FileEventProducer(landing_dir=args.landing_dir)
    if args.target == "kafka":
        from ingestion.producers.kafka_producer import OrderEventProducer

        return OrderEventProducer(topic=args.topic, bootstrap_servers=args.bootstrap_servers)
    from ingestion.producers.eventhub_producer import create_event_hubs_producer

    return create_event_hubs_producer()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic ShopSphere order events.")
    parser.add_argument("--target", choices=["files", "kafka", "eventhubs"], default="files")
    parser.add_argument(
        "--max-events", type=int, default=None, help="Stop after this many ticks (default: run forever)."
    )
    parser.add_argument("--rate", type=float, default=5.0, help="Ticks per second in live mode.")
    parser.add_argument(
        "--backfill-minutes",
        type=int,
        default=None,
        help="Generate as fast as possible, spreading event times over the last N minutes. Requires --max-events.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Make the run reproducible.")
    parser.add_argument("--invalid-rate", type=float, default=config.INVALID_RATE)
    parser.add_argument("--duplicate-rate", type=float, default=config.DUPLICATE_RATE)
    parser.add_argument("--late-rate", type=float, default=config.LATE_RATE)
    parser.add_argument("--landing-dir", default=config.LANDING_DIR)
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    args = parser.parse_args(argv)
    if args.backfill_minutes is not None and args.max_events is None:
        parser.error("--backfill-minutes requires --max-events")
    return args


def run(args: argparse.Namespace) -> dict:
    if args.backfill_minutes is not None:
        span = timedelta(minutes=args.backfill_minutes)
        clock = SimulatedClock(start=utc_now() - span, step=span / args.max_events)
    else:
        clock = utc_now

    generator = EventGenerator(
        seed=args.seed,
        clock=clock,
        invalid_rate=args.invalid_rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
    )
    producer = build_producer(args)
    live = args.backfill_minutes is None
    ticks = 0

    logger.info("Producing to %s (run id %s). Ctrl+C to stop.", args.target, generator.run_id)
    try:
        while args.max_events is None or ticks < args.max_events:
            for event in generator.next_events():
                producer.send(event)
            ticks += 1
            if ticks % 500 == 0:
                logger.info("%s ticks, %s events delivered", ticks, generator.stats["events_delivered"])
            if live:
                time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        logger.info("Stopping.")
    finally:
        # Late events still being held back are delivered now, so nothing the
        # generator counted is lost when the producer stops.
        for event in generator.flush():
            producer.send(event)
        producer.close()

    summary = dict(generator.stats, run_id=generator.run_id, delivery_failures=producer.failed)
    logger.info("Done: %s", json.dumps(summary))
    return summary


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    run(parse_args(argv))


if __name__ == "__main__":
    main()
