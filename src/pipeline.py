"""One-command local pipeline: generate → ETL → DQ → repair → Kafka → report."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .data_generator import generate
from .dq.engine import run_dq
from .etl.local_pipeline import build_warehouse
from .kafka.consistency import run_demo
from .remediation import remediate
from .report import build as build_report

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def run_all(*, reset: bool = True) -> dict[str, Any]:
    started = time.perf_counter()
    manifest = generate(reset=reset)
    etl = build_warehouse()
    dq = run_dq()
    remediation = remediate()
    kafka = run_demo(limit=1000, duplicate_first=True)
    summary = build_report()
    summary.update(
        {
            "seed": manifest["seed"],
            "source_order_rows": etl["source_order_rows"],
            "dwd_rows": etl["dwd_rows"],
            "dws_daily_rows": etl["dws_daily_rows"],
            "dws_channel_rows": etl["dws_channel_rows"],
            "dq_summary": dq["summary"],
            "kafka_passed": kafka["passed"],
            "elapsed_seconds": round(time.perf_counter() - started, 2),
        }
    )
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "pipeline_run.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def main() -> None:
    summary = run_all()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
