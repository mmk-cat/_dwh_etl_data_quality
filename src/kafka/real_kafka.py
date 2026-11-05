"""Real Kafka producer/consumer consistency check (optional integration path).

Prerequisite: a Kafka broker reachable at ``--bootstrap`` and ``kafka-python``.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from .consistency import build_events, check_consistency

ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "reports"


def run(bootstrap: str, topic: str, limit: int = 1000, timeout_seconds: int = 90) -> dict[str, Any]:
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError as exc:
        raise SystemExit("pip install kafka-python") from exc

    events = build_events(limit)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        enable_idempotence=True,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )
    try:
        for event in events:
            producer.send(topic, key=event["event_id"], value=event)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=10)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id="dwh-dq-consistency",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        key_deserializer=lambda key: key.decode("utf-8") if key else "",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    consumed: list[dict[str, Any]] = []
    try:
        deadline = time.time() + timeout_seconds
        while len(consumed) < len(events) and time.time() < deadline:
            records = consumer.poll(timeout_ms=1000)
            for batch in records.values():
                for record in batch:
                    consumed.append(record.value)
    finally:
        consumer.close()

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in consumed:
        if event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        deduped.append(event)
    raw_check = check_consistency(events, consumed)
    deduped_check = check_consistency(events, deduped)
    result = {
        "mode": "real_kafka",
        "bootstrap": bootstrap,
        "topic": topic,
        "raw_consumer": raw_check,
        "idempotent_consumer": deduped_check,
        "duplicates_removed": raw_check["consumed_raw"] - deduped_check["consumed_unique"],
        "passed": deduped_check["passed"],
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "kafka_real_consistency.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="ods-order-events")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()
    print(json.dumps(run(args.bootstrap, args.topic, args.limit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
