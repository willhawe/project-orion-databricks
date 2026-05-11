from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from src.orion.config import (
    CONFIG,
    DONKI_SPACE_WEATHER_SILVER_COLUMNS,
    DONKI_SPACE_WEATHER_SILVER_COLUMN_TYPES,
    bronze_table_name,
    silver_table_name,
)

RAW_DONKI_EVENTS_TABLE = "raw_donki_events"
DONKI_SPACE_WEATHER_EVENTS_TABLE = "donki_space_weather_events"


def raw_donki_events_table_name() -> str:
    return bronze_table_name(RAW_DONKI_EVENTS_TABLE)


def donki_space_weather_events_table_name() -> str:
    return silver_table_name(DONKI_SPACE_WEATHER_EVENTS_TABLE)


def parse_donki_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def event_id_for(event_type: str, event: dict[str, Any]) -> str | None:
    event_specific_id = event.get(f"{event_type.lower()}ID")
    return event_specific_id or event.get("activityID")


def event_time_for(event: dict[str, Any]) -> datetime | None:
    return parse_donki_timestamp(event.get("eventTime") or event.get("beginTime"))


def parse_donki_events(
    *,
    event_type: str,
    response_body: str,
    source_ingestion_run_id: str,
    source_response_hash: str,
    mission_name: str,
    bronze_ingested_at: datetime,
    processed_at: datetime | None = None,
) -> list[dict[str, Any]]:
    processed_at = processed_at or datetime.now(timezone.utc)

    try:
        events = json.loads(response_body)
    except json.JSONDecodeError:
        return []

    if not isinstance(events, list):
        return []

    rows = []
    for event in events:
        if not isinstance(event, dict):
            continue

        rows.append(
            {
                "source_ingestion_run_id": source_ingestion_run_id,
                "source_response_hash": source_response_hash,
                "event_type": event_type,
                "event_id": event_id_for(event_type, event),
                "catalog": event.get("catalog"),
                "event_time": event_time_for(event),
                "event_start_time": parse_donki_timestamp(event.get("beginTime")),
                "event_peak_time": parse_donki_timestamp(event.get("peakTime")),
                "event_end_time": parse_donki_timestamp(event.get("endTime")),
                "submission_time": parse_donki_timestamp(event.get("submissionTime")),
                "version_id": event.get("versionId"),
                "location": event.get("location"),
                "source_location": event.get("sourceLocation"),
                "active_region_number": event.get("activeRegionNum"),
                "class_type": event.get("classType"),
                "link": event.get("link"),
                "instruments_json": json_or_none(event.get("instruments")),
                "linked_events_json": json_or_none(event.get("linkedEvents")),
                "sent_notifications_json": json_or_none(event.get("sentNotifications")),
                "note": event.get("note"),
                "mission_name": mission_name,
                "bronze_ingested_at": bronze_ingested_at,
                "processed_at": processed_at,
            }
        )

    return rows


def silver_schema():
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("source_ingestion_run_id", StringType(), False),
            StructField("source_response_hash", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_id", StringType(), True),
            StructField("catalog", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("event_start_time", TimestampType(), True),
            StructField("event_peak_time", TimestampType(), True),
            StructField("event_end_time", TimestampType(), True),
            StructField("submission_time", TimestampType(), True),
            StructField("version_id", IntegerType(), True),
            StructField("location", StringType(), True),
            StructField("source_location", StringType(), True),
            StructField("active_region_number", IntegerType(), True),
            StructField("class_type", StringType(), True),
            StructField("link", StringType(), True),
            StructField("instruments_json", StringType(), True),
            StructField("linked_events_json", StringType(), True),
            StructField("sent_notifications_json", StringType(), True),
            StructField("note", StringType(), True),
            StructField("mission_name", StringType(), False),
            StructField("bronze_ingested_at", TimestampType(), False),
            StructField("processed_at", TimestampType(), False),
        ]
    )


def create_silver_dataframe(spark, records: list[dict[str, Any]]):
    rows = [[record[column] for column in DONKI_SPACE_WEATHER_SILVER_COLUMNS] for record in records]
    return spark.createDataFrame(rows, silver_schema())


def ensure_silver_table(spark, target_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
          source_ingestion_run_id STRING COMMENT 'Bronze ingestion run ID that produced this source response',
          source_response_hash STRING COMMENT 'SHA-256 hash of the source Bronze response body',
          event_type STRING COMMENT 'DONKI event type, e.g. FLR, IPS, CME',
          event_id STRING COMMENT 'DONKI event identifier',
          catalog STRING COMMENT 'DONKI catalog name',
          event_time TIMESTAMP COMMENT 'Primary event timestamp',
          event_start_time TIMESTAMP COMMENT 'Event start timestamp where provided',
          event_peak_time TIMESTAMP COMMENT 'Event peak timestamp where provided',
          event_end_time TIMESTAMP COMMENT 'Event end timestamp where provided',
          submission_time TIMESTAMP COMMENT 'DONKI submission timestamp',
          version_id INT COMMENT 'DONKI event version ID',
          location STRING COMMENT 'Reported event location',
          source_location STRING COMMENT 'Solar source location where provided',
          active_region_number INT COMMENT 'Solar active region number where provided',
          class_type STRING COMMENT 'Event class, e.g. solar flare class',
          link STRING COMMENT 'DONKI event detail link',
          instruments_json STRING COMMENT 'Source instruments as JSON',
          linked_events_json STRING COMMENT 'Linked DONKI events as JSON',
          sent_notifications_json STRING COMMENT 'Sent DONKI notifications as JSON',
          note STRING COMMENT 'DONKI event note',
          mission_name STRING COMMENT 'Mission name associated with the configured window',
          bronze_ingested_at TIMESTAMP COMMENT 'Timestamp when the source Bronze response was ingested',
          processed_at TIMESTAMP COMMENT 'Timestamp when this Silver row was produced'
        )
        USING DELTA
        COMMENT 'Silver normalized NASA DONKI space-weather events for Project Orion'
        TBLPROPERTIES (
          'project' = 'orion',
          'quality' = 'silver',
          'source' = 'nasa_donki'
        )
        """
    )


def align_silver_dataframe(silver_df):
    from pyspark.sql import functions as F

    return silver_df.select(
        *[
            F.col(column_name).cast(column_type).alias(column_name)
            for column_name, column_type in DONKI_SPACE_WEATHER_SILVER_COLUMN_TYPES
        ]
    )


def insert_silver_dataframe(spark, aligned_df, target_table: str) -> None:
    temp_view = f"tmp_donki_space_weather_events_{uuid.uuid4().hex}"
    column_list = ", ".join(f"`{column}`" for column in DONKI_SPACE_WEATHER_SILVER_COLUMNS)

    aligned_df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            INSERT INTO {target_table} ({column_list})
            SELECT {column_list}
            FROM `{temp_view}`
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)


def transform_donki_space_weather(
    spark,
    *,
    source_table: str | None = None,
    target_table: str | None = None,
) -> dict[str, Any]:
    source_table = source_table or raw_donki_events_table_name()
    target_table = target_table or donki_space_weather_events_table_name()

    bronze_rows = spark.sql(
        f"""
        SELECT
            ingestion_run_id,
            response_hash,
            event_type,
            response_body,
            mission_name,
            ingested_at
        FROM {source_table}
        WHERE response_status_code = 200
        """
    ).collect()

    records: list[dict[str, Any]] = []
    for row in bronze_rows:
        records.extend(
            parse_donki_events(
                event_type=row["event_type"],
                response_body=row["response_body"],
                source_ingestion_run_id=row["ingestion_run_id"],
                source_response_hash=row["response_hash"],
                mission_name=row["mission_name"] or CONFIG.mission_name,
                bronze_ingested_at=row["ingested_at"],
            )
        )

    ensure_silver_table(spark, target_table)
    if not records:
        return {
            "source_table": source_table,
            "target_table": target_table,
            "bronze_records_read": len(bronze_rows),
            "silver_records_written": 0,
        }

    source_ids = sorted({record["source_ingestion_run_id"] for record in records})
    quoted_ids = ", ".join(f"'{source_id}'" for source_id in source_ids)
    spark.sql(f"DELETE FROM {target_table} WHERE source_ingestion_run_id IN ({quoted_ids})")

    silver_df = create_silver_dataframe(spark, records)
    aligned_df = align_silver_dataframe(silver_df)
    records_to_write = aligned_df.count()
    insert_silver_dataframe(spark, aligned_df, target_table)

    return {
        "source_table": source_table,
        "target_table": target_table,
        "bronze_records_read": len(bronze_rows),
        "silver_records_written": records_to_write,
    }
