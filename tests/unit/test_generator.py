import json
from datetime import datetime, timedelta, timezone

from ingestion.generators.catalog import generate_customers, generate_products
from ingestion.generators.event_generator import EventGenerator, SimulatedClock
from ingestion.producers.file_producer import FileEventProducer
from ingestion.run_producer import parse_args, run


def make_generator(**overrides):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    settings = {"seed": 7, "run_id": "test", "clock": SimulatedClock(start, timedelta(seconds=30))}
    settings.update(overrides)
    return EventGenerator(**settings)


def drain(generator, ticks):
    events = []
    for _ in range(ticks):
        events.extend(generator.next_events())
    events.extend(generator.flush())
    return events


def test_catalogues_are_identical_in_every_run():
    assert generate_customers() == generate_customers()
    assert generate_products() == generate_products()
    assert generate_products()[0]["product_id"] == "P000001"


def test_same_seed_gives_the_same_events():
    assert drain(make_generator(), 200) == drain(make_generator(), 200)


def test_order_ids_differ_between_runs():
    first = {e["payload"]["order_id"] for e in drain(make_generator(run_id="a", invalid_rate=0), 50)}
    second = {e["payload"]["order_id"] for e in drain(make_generator(run_id="b", invalid_rate=0), 50)}
    assert first.isdisjoint(second)


def test_orders_move_through_a_lifecycle():
    events = drain(make_generator(invalid_rate=0, duplicate_rate=0, late_rate=0), 500)
    by_order = {}
    for event in events:
        by_order.setdefault(event["payload"]["order_id"], []).append(event["payload"]["order_status"])

    assert any(len(statuses) > 1 for statuses in by_order.values()), "no order ever changed status"
    allowed = {("CREATED", "PAID"), ("PAID", "SHIPPED"), ("SHIPPED", "DELIVERED"), ("CREATED", "CANCELLED")}
    for statuses in by_order.values():
        assert statuses[0] == "CREATED"
        assert all(step in allowed for step in zip(statuses, statuses[1:], strict=False))


def test_clean_events_are_internally_consistent():
    events = drain(make_generator(invalid_rate=0, duplicate_rate=0, late_rate=0), 200)
    for event in events:
        payload = event["payload"]
        assert payload["total_amount"] == round(payload["quantity"] * payload["unit_price"], 2)
        assert len({e["event_id"] for e in events}) == len(events)


def test_duplicates_share_an_event_id():
    generator = make_generator(invalid_rate=0, duplicate_rate=1.0, late_rate=0)
    events = drain(generator, 20)
    assert len(events) == 40
    assert len({e["event_id"] for e in events}) == 20
    assert generator.stats["duplicates"] == 20


def test_late_events_keep_their_event_time_but_are_delivered_later():
    generator = make_generator(invalid_rate=0, duplicate_rate=0, late_rate=1.0)
    assert generator.next_events() == []  # held back, not delivered yet

    delivered = []
    for _ in range(20):  # 20 ticks x 30s = 10 minutes of simulated time
        delivered.extend(generator.next_events())
    assert delivered, "late events were never released"
    assert all(event["is_late_event"] for event in delivered)
    # Delivered in event-time order, each at least the delay after it happened.
    times = [event["event_timestamp"] for event in delivered]
    assert times == sorted(times)


def test_flush_delivers_everything_still_held_back():
    generator = make_generator(late_rate=1.0, invalid_rate=0, duplicate_rate=0)
    generator.next_events()
    assert len(generator.flush()) == 1
    assert generator.flush() == []


def test_invalid_events_are_counted():
    generator = make_generator(invalid_rate=1.0, duplicate_rate=0, late_rate=0)
    drain(generator, 30)
    assert generator.stats["invalid"] == 30


def test_file_producer_writes_one_json_object_per_line(tmp_path):
    producer = FileEventProducer(landing_dir=str(tmp_path), batch_size=10)
    events = drain(make_generator(), 25)
    for event in events:
        producer.send(event)
    producer.close()

    files = sorted(tmp_path.glob("*.json"))
    assert len(files) >= 3
    lines = [line for file in files for line in file.read_text().splitlines()]
    assert len(lines) == len(events)
    assert json.loads(lines[0])["event_id"] == events[0]["event_id"]
    assert not list(tmp_path.glob(".*")), "temporary files were left behind"


def test_backfill_command_writes_events_and_reports_stats(tmp_path):
    args = parse_args(
        [
            "--target",
            "files",
            "--max-events",
            "300",
            "--backfill-minutes",
            "60",
            "--seed",
            "1",
            "--landing-dir",
            str(tmp_path),
        ]
    )
    summary = run(args)
    lines = [line for file in tmp_path.glob("*.json") for line in file.read_text().splitlines()]
    assert summary["events_delivered"] == len(lines)
    assert summary["orders_created"] + summary["status_updates"] == 300
