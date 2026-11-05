"""Kafka-style producer/consumer consistency check.

A file-backed bus keeps the exercise runnable without a broker while exercising
the same contract: event ids, checksums, at-least-once duplicates, bounded lag
and sequence ordering.  ``real_kafka.py`` talks to a real Kafka cluster.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CLEAN_DWD = ROOT / "data" / "warehouse_clean" / "dwd_order_detail.csv"
KAFKA_DIR = ROOT / "data" / "kafka"
REPORTS_DIR = ROOT / "reports"


def build_events(limit: int = 1000) -> list[dict[str, Any]]:
    if not CLEAN_DWD.exists():
        raise FileNotFoundError("Run remediation first: data/warehouse_clean/dwd_order_detail.csv")
    dwd = pd.read_csv(CLEAN_DWD, dtype=str, keep_default_na=False, encoding="utf-8")
    dwd = dwd.sort_values("order_id").head(limit)
    events = []
    produced_at = datetime.now().astimezone().isoformat(timespec="milliseconds")
    for sequence, (_, row) in enumerate(dwd.iterrows(), start=1):
        payload = {
            "order_id": row["order_id"],
            "user_id": row["user_id"],
            "product_id": row["product_id"],
            "category": row["category"],
            "quantity": int(float(row["quantity"] or 0)),
            "amount": round(float(row["amount"] or 0), 2),
            "order_date": row["order_date"],
        }
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        events.append(
            {
                "event_id": f"evt-{sequence:06d}",
                "sequence": sequence,
                "order_id": row["order_id"],
                "payload": payload,
                "payload_checksum": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
                "produced_at": produced_at,
            }
        )
    return events


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def check_consistency(
    produced: list[dict[str, Any]], consumed: list[dict[str, Any]]
) -> dict[str, Any]:
    produced_by_id = {event["event_id"]: event for event in produced}
    consumed_by_id: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for event in consumed:
        if event["event_id"] in consumed_by_id:
            duplicates += 1
        consumed_by_id[event["event_id"]] = event
    missing = sorted(set(produced_by_id) - set(consumed_by_id))
    extra = sorted(set(consumed_by_id) - set(produced_by_id))
    checksum_mismatch = [
        event_id
        for event_id, event in consumed_by_id.items()
        if event_id in produced_by_id
        and event["payload_checksum"] != produced_by_id[event_id]["payload_checksum"]
    ]
    consumed_sequences = [int(event["sequence"]) for event in consumed]
    order_ok = consumed_sequences == sorted(consumed_sequences)
    return {
        "produced": len(produced),
        "consumed_raw": len(consumed),
        "consumed_unique": len(consumed_by_id),
        "duplicate_messages": duplicates,
        "missing": len(missing),
        "missing_ids": missing[:10],
        "extra": len(extra),
        "extra_ids": extra[:10],
        "checksum_mismatch": len(checksum_mismatch),
        "checksum_mismatch_ids": checksum_mismatch[:10],
        "order_preserved": order_ok,
        "lag": len(missing),
        "passed": not missing and not extra and not checksum_mismatch and order_ok,
    }


def run_demo(*, limit: int = 1000, duplicate_first: bool = True) -> dict[str, Any]:
    events = build_events(limit)
    KAFKA_DIR.mkdir(parents=True, exist_ok=True)
    _write_jsonl(KAFKA_DIR / "producer.jsonl", events)

    raw = [dict(event) for event in events]
    if duplicate_first:
        raw.insert(1, dict(events[0]))  # simulate at-least-once redelivery
    _write_jsonl(KAFKA_DIR / "consumer_raw.jsonl", raw)

    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for event in raw:
        if event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        deduped.append(event)
    _write_jsonl(KAFKA_DIR / "consumer_deduped.jsonl", deduped)

    raw_check = check_consistency(events, raw)
    deduped_check = check_consistency(events, deduped)
    result = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "mode": "local_file_bus",
        "semantics": "at-least-once + idempotent consumer dedupe",
        "topic": "ods-order-events",
        "limit": limit,
        "producer_file": str((KAFKA_DIR / "producer.jsonl").relative_to(ROOT)),
        "consumer_file": str((KAFKA_DIR / "consumer_deduped.jsonl").relative_to(ROOT)),
        "raw_consumer": raw_check,
        "idempotent_consumer": deduped_check,
        "duplicates_removed": raw_check["consumed_raw"] - deduped_check["consumed_unique"],
        "passed": deduped_check["passed"] and raw_check["missing"] == 0,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "kafka_consistency.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    print(json.dumps(run_demo(), ensure_ascii=False, indent=2))
