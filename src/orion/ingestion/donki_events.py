from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable

from src.orion.config import (
    CONFIG,
    DONKI_BRONZE_COLUMNS,
    DONKI_BRONZE_COLUMN_TYPES,
    bronze_table_name,
)

SOURCE_SYSTEM = "nasa_donki"
RAW_DONKI_EVENTS_TABLE = "raw_donki_events"
DEFAULT_EVENT_TYPES = ("CME", "FLR", "GST", "IPS", "SEP")


def raw_donki_events_table_name() -> str:
    return bronze_table_name(RAW_DONKI_EVENTS_TABLE)


def build_request_params(api_key: str | None = None) -> dict[str, str]:
    return {
        "startDate": CONFIG.mission_start_date,
        "endDate": CONFIG.mission_end_date,
        "api_key": api_key or os.getenv("NASA_API_KEY", "DEMO_KEY"),
    }


def build_endpoint_url(event_type: str) -> str:
    return f"{CONFIG.donki_base_url}/{event_type}"


def build_bronze_record(
    *,
    event_type: str,
    response_body: str,
    response_status_code: int,
    request_url: str,
    request_params: dict[str, str] | None = None,
    ingestion_run_id: str | None = None,
    ingested_at: datetime | None = None,
) -> dict[str, Any]:
    ingested_at = ingested_at or datetime.now(timezone.utc)
    params = request_params or build_request_params()

    return {
        "ingestion_run_id": ingestion_run_id or str(uuid.uuid4()),
        "source_system": SOURCE_SYSTEM,
        "source_endpoint": event_type,
        "event_type": event_type,
        "request_url": request_url,
        "request_params_json": json.dumps(params, sort_keys=True),
        "response_status_code": int(response_status_code),
        "response_body": response_body,
        "response_hash": hashlib.sha256(response_body.encode("utf-8")).hexdigest(),
        "ingested_at": ingested_at,
        "ingested_date": ingested_at.date(),
        "mission_name": CONFIG.mission_name,
        "mission_start_date": date.fromisoformat(CONFIG.mission_start_date),
        "mission_end_date": date.fromisoformat(CONFIG.mission_end_date),
    }


def bronze_schema():
    from pyspark.sql.types import (
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("ingestion_run_id", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("source_endpoint", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("request_url", StringType(), False),
            StructField("request_params_json", StringType(), False),
            StructField("response_status_code", IntegerType(), False),
            StructField("response_body", StringType(), True),
            StructField("response_hash", StringType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("ingested_date", DateType(), False),
            StructField("mission_name", StringType(), False),
            StructField("mission_start_date", DateType(), False),
            StructField("mission_end_date", DateType(), False),
        ]
    )


def create_bronze_dataframe(spark, records: list[dict[str, Any]]):
    rows = [[record[column] for column in DONKI_BRONZE_COLUMNS] for record in records]
    return spark.createDataFrame(rows, bronze_schema())


def ensure_target_table_contract(spark, target_table: str) -> None:
    existing_columns = set(spark.table(target_table).columns)
    missing_columns = [column for column in ("event_type", "ingested_date") if column not in existing_columns]

    if not missing_columns:
        return

    column_definitions = {
        "event_type": "event_type STRING COMMENT 'Space weather event type requested from DONKI'",
        "ingested_date": "ingested_date DATE COMMENT 'Date derived from ingested_at for filtering'",
    }
    add_columns_sql = ", ".join(column_definitions[column] for column in missing_columns)

    spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({add_columns_sql})")


def response_hash_exists(spark, target_table: str, event_type: str, response_hash: str) -> bool:
    from pyspark.sql import functions as F

    return (
        spark.table(target_table)
        .where(
            (F.col("event_type") == event_type)
            & (F.col("source_endpoint") == event_type)
            & (F.col("response_hash") == response_hash)
        )
        .limit(1)
        .count()
        > 0
    )


def align_bronze_dataframe(bronze_df):
    from pyspark.sql import functions as F

    return bronze_df.select(
        *[
            F.col(column_name).cast(column_type).alias(column_name)
            for column_name, column_type in DONKI_BRONZE_COLUMN_TYPES
        ]
    )


def insert_bronze_dataframe(spark, aligned_df, target_table: str) -> None:
    temp_view = f"tmp_raw_donki_events_{uuid.uuid4().hex}"
    column_list = ", ".join(f"`{column}`" for column in DONKI_BRONZE_COLUMNS)

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


def append_new_responses(spark, bronze_df, target_table: str) -> int:
    rows = bronze_df.select("event_type", "response_hash").collect()
    new_rows = [
        row
        for row in rows
        if not response_hash_exists(spark, target_table, row["event_type"], row["response_hash"])
    ]
    if not new_rows:
        return 0

    new_hashes = {(row["event_type"], row["response_hash"]) for row in new_rows}
    from pyspark.sql import functions as F

    filter_condition = None
    for event_type, response_hash in new_hashes:
        pair_condition = (F.col("event_type") == event_type) & (F.col("response_hash") == response_hash)
        filter_condition = pair_condition if filter_condition is None else filter_condition | pair_condition

    filtered_df = bronze_df.where(filter_condition)
    aligned_df = align_bronze_dataframe(filtered_df)
    records_to_write = aligned_df.count()
    insert_bronze_dataframe(spark, aligned_df, target_table)
    return records_to_write


def default_http_get(url: str, *, params: dict[str, str], timeout: int):
    import requests

    return requests.get(url, params=params, timeout=timeout)


def ingest_donki_events(
    spark,
    *,
    http_get: Callable[..., Any] = default_http_get,
    target_table: str | None = None,
    event_types: tuple[str, ...] = DEFAULT_EVENT_TYPES,
    timeout_seconds: int = 60,
    api_key: str | None = None,
) -> dict[str, Any]:
    request_params = build_request_params(api_key=api_key)
    ingestion_run_id = str(uuid.uuid4())
    records = []

    for event_type in event_types:
        response = http_get(build_endpoint_url(event_type), params=request_params, timeout=timeout_seconds)
        records.append(
            build_bronze_record(
                event_type=event_type,
                response_body=response.text,
                response_status_code=response.status_code,
                request_url=response.url,
                request_params=request_params,
                ingestion_run_id=ingestion_run_id,
            )
        )

    bronze_df = create_bronze_dataframe(spark, records)
    table_name = target_table or raw_donki_events_table_name()
    ensure_target_table_contract(spark, table_name)
    records_written = append_new_responses(spark, bronze_df, table_name)

    return {
        "ingestion_run_id": ingestion_run_id,
        "event_types": event_types,
        "records_requested": len(records),
        "records_written": records_written,
        "target_table": table_name,
        "response_status_codes": {
            record["event_type"]: record["response_status_code"] for record in records
        },
    }
