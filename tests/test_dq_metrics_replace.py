"""write_dq_metrics의 replace(재실행 멱등) 계약 — 로컬 Iceberg로 검증."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyiceberg.catalog.sql import SqlCatalog

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from oliveyoung_common import dq_metrics  # noqa: E402
from oliveyoung_common.dq_metrics import DQ_METRICS_TABLE, create_dq_metrics_table, write_dq_metrics  # noqa: E402


class DqMetricsReplaceTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self._tmp.name).as_posix()
        self.catalog = SqlCatalog("t", uri=f"sqlite:///{root}/c.db", warehouse=f"file://{root}/w")
        self.catalog.create_namespace("oliveyoung_db")
        create_dq_metrics_table(self.catalog)

    def tearDown(self):
        self._tmp.cleanup()

    def rows(self):
        t = self.catalog.load_table(DQ_METRICS_TABLE).scan().to_arrow()
        return sorted(zip(t.column("stage").to_pylist(), t.column("batch_date").to_pylist(),
                          t.column("run_id").to_pylist(), t.column("metric_name").to_pylist()))

    def test_default_appends(self):
        for _ in range(2):
            self.assertEqual(write_dq_metrics(self.catalog, "crawl", "2026-10-01", "r1", products_total=10), 0)
        self.assertEqual(len(self.rows()), 2)

    def test_other_runs_keeps_same_run_calls_and_replaces_previous_run(self):
        kw = dict(stage="bronze_to_silver", batch_date="2026-10-01", replace="other_runs")
        # 다른 날짜·다른 stage·백필은 건드리지 않아야 함
        write_dq_metrics(self.catalog, "bronze_to_silver", "2026-09-28", "old", silver_ok=1)
        write_dq_metrics(self.catalog, "bronze_to_silver_backfill", "2026-10-01", "backfill_x", silver_ok=1)
        # 첫 실행: 요약 + 유형별 두 번 호출
        self.assertEqual(write_dq_metrics(self.catalog, run_id="run1", silver_ok=5, silver_error=2, **kw), 0)
        self.assertEqual(write_dq_metrics(self.catalog, run_id="run1", err_A=2, **kw), 0)
        # 재실행
        self.assertEqual(write_dq_metrics(self.catalog, run_id="run2", silver_ok=5, silver_error=2, **kw), 3)
        self.assertEqual(write_dq_metrics(self.catalog, run_id="run2", err_A=2, **kw), 0)
        self.assertEqual(self.rows(), [
            ("bronze_to_silver", "2026-09-28", "old", "silver_ok"),
            ("bronze_to_silver", "2026-10-01", "run2", "err_A"),
            ("bronze_to_silver", "2026-10-01", "run2", "silver_error"),
            ("bronze_to_silver", "2026-10-01", "run2", "silver_ok"),
            ("bronze_to_silver_backfill", "2026-10-01", "backfill_x", "silver_ok"),
        ])

    def test_same_run_replaces_fixed_key(self):
        kw = dict(stage="bronze_to_silver_backfill", batch_date="2026-08-01", run_id="backfill_20260731",
                  replace="same_run")
        write_dq_metrics(self.catalog, "bronze_to_silver_backfill", "2026-08-01", "backfill_other", silver_ok=9)
        write_dq_metrics(self.catalog, silver_ok=1, err_A=1, **kw)
        self.assertEqual(write_dq_metrics(self.catalog, silver_ok=2, **kw), 2)
        self.assertEqual(self.rows(), [
            ("bronze_to_silver_backfill", "2026-08-01", "backfill_20260731", "silver_ok"),
            ("bronze_to_silver_backfill", "2026-08-01", "backfill_other", "silver_ok"),
        ])

    def test_invalid_replace_rejected(self):
        with self.assertRaises(ValueError):
            write_dq_metrics(self.catalog, "crawl", "2026-10-01", "r1", replace="all", x=1)

    def test_report_marks_rerun(self):
        sent = []
        with patch.object(dq_metrics.urllib.request, "urlopen",
                          side_effect=lambda req, timeout: sent.append(json.loads(req.data)["content"])):
            kw = dict(stage="bronze_to_silver", batch_date="2026-10-01", replace="other_runs",
                      report_webhook="https://example.invalid/hook")
            write_dq_metrics(self.catalog, run_id="run1", silver_ok=1, **kw)
            write_dq_metrics(self.catalog, run_id="run2", silver_ok=1, **kw)
        self.assertNotIn("(재실행)", sent[0].splitlines()[0])
        self.assertIn("(재실행)", sent[1].splitlines()[0])


if __name__ == "__main__":
    unittest.main()
