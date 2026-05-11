from datetime import datetime, timezone

from src.orion.config import (
    DONKI_SPACE_WEATHER_SILVER_COLUMNS,
    DONKI_SPACE_WEATHER_SILVER_COLUMN_TYPES,
)
from src.orion.transforms.donki_space_weather import (
    build_deduplicated_insert_sql,
    donki_space_weather_events_table_name,
    parse_donki_events,
    parse_donki_timestamp,
)

SAMPLE_FLR_RESPONSE = """
[
  {
    "flrID": "2026-04-01T13:38:00-FLR-001",
    "catalog": "M2M_CATALOG",
    "instruments": [{"displayName": "GOES-P: EXIS 1.0-8.0"}],
    "beginTime": "2026-04-01T13:38Z",
    "peakTime": "2026-04-01T13:48Z",
    "endTime": "2026-04-01T13:55Z",
    "classType": "C5.3",
    "sourceLocation": "N02E30",
    "activeRegionNum": 14409,
    "note": "",
    "submissionTime": "2026-04-01T20:29Z",
    "versionId": 1,
    "link": "https://webtools.ccmc.gsfc.nasa.gov/DONKI/view/FLR/45473/-1",
    "linkedEvents": [{"activityID": "2026-04-01T15:48:00-CME-001"}],
    "sentNotifications": null
  }
]
"""

SAMPLE_IPS_RESPONSE = """
[
  {
    "catalog": "M2M_CATALOG",
    "activityID": "2026-04-01T11:29:00-IPS-001",
    "location": "Earth",
    "eventTime": "2026-04-01T11:29Z",
    "submissionTime": "2026-04-10T17:05Z",
    "versionId": 3,
    "link": "https://webtools.ccmc.gsfc.nasa.gov/DONKI/view/IPS/45460/-1",
    "instruments": [{"displayName": "ACE: MAG"}],
    "linkedEvents": [{"activityID": "2026-03-30T03:24:00-CME-001"}],
    "sentNotifications": [{"messageID": "20260401-AL-003"}]
  }
]
"""


def test_parse_donki_timestamp_handles_zulu_time():
    assert parse_donki_timestamp("2026-04-01T13:38Z") == datetime(2026, 4, 1, 13, 38, tzinfo=timezone.utc)


def test_parse_donki_events_flare_response():
    rows = parse_donki_events(
        event_type="FLR",
        response_body=SAMPLE_FLR_RESPONSE,
        source_ingestion_run_id="run-1",
        source_response_hash="hash-1",
        mission_name="artemis_ii",
        bronze_ingested_at=datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
        processed_at=datetime(2026, 5, 11, 13, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert tuple(rows[0].keys()) == DONKI_SPACE_WEATHER_SILVER_COLUMNS
    assert rows[0]["event_type"] == "FLR"
    assert rows[0]["event_id"] == "2026-04-01T13:38:00-FLR-001"
    assert rows[0]["event_time"] == datetime(2026, 4, 1, 13, 38, tzinfo=timezone.utc)
    assert rows[0]["event_peak_time"] == datetime(2026, 4, 1, 13, 48, tzinfo=timezone.utc)
    assert rows[0]["class_type"] == "C5.3"
    assert rows[0]["active_region_number"] == 14409
    assert "GOES-P" in rows[0]["instruments_json"]


def test_parse_donki_events_ips_response():
    rows = parse_donki_events(
        event_type="IPS",
        response_body=SAMPLE_IPS_RESPONSE,
        source_ingestion_run_id="run-1",
        source_response_hash="hash-1",
        mission_name="artemis_ii",
        bronze_ingested_at=datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert rows[0]["event_id"] == "2026-04-01T11:29:00-IPS-001"
    assert rows[0]["event_time"] == datetime(2026, 4, 1, 11, 29, tzinfo=timezone.utc)
    assert rows[0]["location"] == "Earth"
    assert rows[0]["class_type"] is None


def test_parse_donki_events_empty_or_invalid_response():
    assert parse_donki_events(
        event_type="SEP",
        response_body="[]",
        source_ingestion_run_id="run-1",
        source_response_hash="hash-1",
        mission_name="artemis_ii",
        bronze_ingested_at=datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
    ) == []
    assert parse_donki_events(
        event_type="CME",
        response_body="not json",
        source_ingestion_run_id="run-1",
        source_response_hash="hash-1",
        mission_name="artemis_ii",
        bronze_ingested_at=datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
    ) == []


def test_donki_space_weather_events_table_name():
    assert (
        donki_space_weather_events_table_name()
        == "dbw_orion_dev_uks_001.orion_silver.donki_space_weather_events"
    )


def test_donki_space_weather_silver_column_types_align_with_columns():
    assert tuple(column for column, _ in DONKI_SPACE_WEATHER_SILVER_COLUMN_TYPES) == DONKI_SPACE_WEATHER_SILVER_COLUMNS


def test_build_deduplicated_insert_sql_uses_event_identity_and_latest_bronze_row():
    sql = build_deduplicated_insert_sql(source_view="tmp_events", target_table="silver.events")

    assert "INSERT INTO silver.events" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY mission_name, event_type" in sql
    assert "COALESCE(" in sql
    assert "event_id" in sql
    assert "ORDER BY bronze_ingested_at DESC" in sql
    assert "WHERE row_number = 1" in sql
