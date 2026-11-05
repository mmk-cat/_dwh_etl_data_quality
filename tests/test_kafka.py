"""Kafka producer/consumer consistency contract tests."""

from __future__ import annotations

import copy

import pytest

from src.kafka.consistency import check_consistency

pytestmark = [pytest.mark.kafka]


def test_producer_event_count(kafka):
    assert kafka["idempotent_consumer"]["produced"] == 1000


def test_raw_consumer_has_at_least_once_duplicate(kafka):
    assert kafka["raw_consumer"]["consumed_raw"] == 1001
    assert kafka["raw_consumer"]["duplicate_messages"] == 1
    assert kafka["duplicates_removed"] == 1


def test_idempotent_consumer_has_one_thousand_unique_events(kafka):
    assert kafka["idempotent_consumer"]["consumed_unique"] == 1000
    assert kafka["idempotent_consumer"]["duplicate_messages"] == 0
    assert kafka["idempotent_consumer"]["lag"] == 0


def test_no_missing_or_extra_messages(kafka):
    for key in ("raw_consumer", "idempotent_consumer"):
        assert kafka[key]["missing"] == 0
        assert kafka[key]["extra"] == 0
        assert kafka[key]["missing_ids"] == []
        assert kafka[key]["extra_ids"] == []


def test_checksums_and_order(kafka):
    for key in ("raw_consumer", "idempotent_consumer"):
        assert kafka[key]["checksum_mismatch"] == 0
        assert kafka[key]["order_preserved"] is True
        assert kafka[key]["passed"] is True


def test_consistency_checker_detects_tampering():
    produced = [
        {"event_id": "e1", "sequence": 1, "payload_checksum": "abc"},
        {"event_id": "e2", "sequence": 2, "payload_checksum": "def"},
    ]
    consumed = copy.deepcopy(produced)
    consumed[1]["payload_checksum"] = "tampered"
    result = check_consistency(produced, consumed)
    assert result["checksum_mismatch"] == 1
    assert result["checksum_mismatch_ids"] == ["e2"]
    assert result["passed"] is False


def test_demo_overall_pass(kafka):
    assert kafka["passed"] is True
    assert kafka["mode"] == "local_file_bus"
    assert kafka["semantics"] == "at-least-once + idempotent consumer dedupe"
