"""Unit tests for nyc_taxi_pipeline DAG callables.

Airflow + provider modules are not required at test time; we stub them so the
module imports cleanly in CI without `apache-airflow` installed.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Environment + module stubs MUST be set before importing the DAG.
os.environ.setdefault("GCP_PROJECT_ID", "test-project")

_AIRFLOW_STUBS = [
    "airflow",
    "airflow.operators.python",
    "airflow.providers.google.cloud.transfers.local_to_gcs",
    "airflow.providers.google.cloud.operators.bigquery",
]
for name in _AIRFLOW_STUBS:
    sys.modules.setdefault(name, MagicMock())

# Make the DAG module importable regardless of pytest's CWD.
sys.path.insert(0, str(Path(__file__).parent))

import nyc_taxi_pipeline  # noqa: E402


class TestHelpers(unittest.TestCase):
    def test_year_month_valid(self):
        self.assertEqual(nyc_taxi_pipeline._year_month("2023-01"), ("2023", "01"))

    def test_year_month_invalid(self):
        with self.assertRaises(ValueError):
            nyc_taxi_pipeline._year_month("2023-1")
        with self.assertRaises(ValueError):
            nyc_taxi_pipeline._year_month("not-a-date")

    def test_run_dir_sanitizes(self):
        d = nyc_taxi_pipeline._run_dir("manual__2026-05-22T10:30:00+00:00")
        self.assertNotIn(":", d)
        self.assertNotIn("+", d)
        self.assertTrue(d.startswith(nyc_taxi_pipeline.DOWNLOAD_ROOT))


class TestDownloadTaxiData(unittest.TestCase):
    @patch("nyc_taxi_pipeline.requests.get")
    def test_downloads_each_month_streamed(self, mock_get):
        # Build a streaming response mock that yields 2 MB so the size check passes.
        chunk = b"x" * (1024 * 1024)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.iter_content.return_value = [chunk, chunk]
        mock_get.return_value.__enter__.return_value = mock_resp

        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(nyc_taxi_pipeline, "DOWNLOAD_ROOT", tmp):
            result = nyc_taxi_pipeline.download_taxi_data(
                run_id="test_run",
                params={"months": ["2023-01", "2023-02"]},
            )

            self.assertTrue(os.path.isdir(result))
            self.assertEqual(mock_get.call_count, 2)
            for month in ("2023-01", "2023-02"):
                expected = Path(result) / f"yellow_tripdata_{month}.parquet"
                self.assertTrue(expected.exists(), f"missing {expected}")
                self.assertGreaterEqual(expected.stat().st_size, 1_000_000)

    @patch("nyc_taxi_pipeline.requests.get")
    def test_raises_on_tiny_file(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.iter_content.return_value = [b"too small"]
        mock_get.return_value.__enter__.return_value = mock_resp

        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(nyc_taxi_pipeline, "DOWNLOAD_ROOT", tmp):
            with self.assertRaises(RuntimeError):
                nyc_taxi_pipeline.download_taxi_data(
                    run_id="test_run",
                    params={"months": ["2023-01"]},
                )


class TestCleanup(unittest.TestCase):
    def test_cleanup_removes_run_dir(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(nyc_taxi_pipeline, "DOWNLOAD_ROOT", tmp):
            run_dir = nyc_taxi_pipeline._run_dir("rid")
            os.makedirs(run_dir, exist_ok=True)
            Path(run_dir, "x.parquet").write_bytes(b"junk")

            nyc_taxi_pipeline.cleanup_download_dir(run_id="rid")
            self.assertFalse(os.path.exists(run_dir))

    def test_cleanup_is_idempotent(self):
        # Should not raise even if directory is already gone.
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(nyc_taxi_pipeline, "DOWNLOAD_ROOT", tmp):
            nyc_taxi_pipeline.cleanup_download_dir(run_id="never_created")


class TestExternalTableResource(unittest.TestCase):
    def test_source_uris_use_bucket_and_months(self):
        uris = nyc_taxi_pipeline._external_source_uris(["2023-01"])
        self.assertEqual(len(uris), 1)
        self.assertIn(nyc_taxi_pipeline.BUCKET_NAME, uris[0])
        self.assertTrue(uris[0].endswith("yellow_tripdata_2023-01.parquet"))

    def test_resource_dict_shape(self):
        r = nyc_taxi_pipeline.EXTERNAL_TABLE_RESOURCE
        self.assertEqual(
            r["tableReference"]["projectId"], nyc_taxi_pipeline.PROJECT_ID
        )
        self.assertEqual(r["externalDataConfiguration"]["sourceFormat"], "PARQUET")
        self.assertGreater(len(r["externalDataConfiguration"]["schema"]["fields"]), 0)


if __name__ == "__main__":
    unittest.main()
