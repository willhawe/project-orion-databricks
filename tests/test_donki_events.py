from datetime import datetime, timezone

from src.orion.config import CONFIG, DONKI_BRONZE_COLUMNS, DONKI_BRONZE_COLUMN_TYPES
from src.orion.ingestion.donki_events import (
    DEFAULT_EVENT_TYPES,
    align_bronze_dataframe,
    build_bronze_record,
    build_endpoint_url,
    build_request_params,
    raw_donki_events_table_name,
)


def test_build_request_params_uses_configured_mission_window():
    params = build_request_params(api_key="test-key")

    assert params["startDate"] == CONFIG.mission_start_date
    assert params["endDate"] == CONFIG.mission_end_date
    assert params["api_key"] == "test-key"


def test_default_event_types_are_expected_donki_endpoints():
    assert DEFAULT_EVENT_TYPES == ("CME", "FLR", "GST", "IPS", "SEP")
    assert build_endpoint_url("CME") == "https://api.nasa.gov/DONKI/CME"


def test_build_bronze_record_contains_expected_columns():
    record = build_bronze_record(
        event_type="CME",
        response_body='[{"activityID": "test"}]',
        response_status_code=200,
        request_url="https://api.nasa.gov/DONKI/CME?startDate=2026-04-01",
        request_params=build_request_params(api_key="test-key"),
        ingestion_run_id="test-run",
        ingested_at=datetime(2026, 4, 1, 12, 30, tzinfo=timezone.utc),
    )

    assert tuple(record.keys()) == DONKI_BRONZE_COLUMNS
    assert record["ingestion_run_id"] == "test-run"
    assert record["source_system"] == "nasa_donki"
    assert record["source_endpoint"] == "CME"
    assert record["event_type"] == "CME"
    assert record["ingested_date"].isoformat() == "2026-04-01"
    assert record["mission_start_date"].isoformat() == CONFIG.mission_start_date
    assert record["mission_end_date"].isoformat() == CONFIG.mission_end_date


def test_raw_donki_events_table_name():
    assert (
        raw_donki_events_table_name()
        == "dbw_orion_dev_uks_001.orion_bronze.raw_donki_events"
    )


def test_donki_bronze_column_types_align_with_columns():
    assert tuple(column for column, _ in DONKI_BRONZE_COLUMN_TYPES) == DONKI_BRONZE_COLUMNS


def test_align_bronze_dataframe_selects_columns_in_contract_order(monkeypatch):
    selected_columns = []

    class FakeColumn:
        def __init__(self, name):
            self.name = name
            self.column_type = None

        def cast(self, column_type):
            self.column_type = column_type
            return self

        def alias(self, name):
            selected_columns.append((self.name, self.column_type, name))
            return self

    class FakeFunctions:
        @staticmethod
        def col(name):
            return FakeColumn(name)

    class FakeDataFrame:
        def select(self, *columns):
            return columns

    monkeypatch.setitem(__import__("sys").modules, "pyspark", type("FakePySpark", (), {})())
    monkeypatch.setitem(__import__("sys").modules, "pyspark.sql", type("FakeSql", (), {"functions": FakeFunctions})())

    align_bronze_dataframe(FakeDataFrame())

    assert tuple(column for column, _, _ in selected_columns) == DONKI_BRONZE_COLUMNS
    assert all(column == alias for column, _, alias in selected_columns)
