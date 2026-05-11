from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from src.orion.config import (
    CONFIG,
    JPL_EPHEMERIS_SILVER_COLUMNS,
    JPL_EPHEMERIS_SILVER_COLUMN_TYPES,
    bronze_table_name,
    silver_table_name,
)

RAW_JPL_HORIZONS_TABLE = "raw_jpl_horizons"
JPL_EPHEMERIS_VECTORS_TABLE = "jpl_ephemeris_vectors"
TARGET_BODY = "moon"
CENTER = "earth"

VECTOR_ROW_PATTERN = re.compile(
    r"^\s*"
    r"(?P<julian_date>[0-9]+\.[0-9]+)\s*,\s*"
    r"A\.D\.\s*(?P<timestamp>[0-9]{4}-[A-Za-z]{3}-[0-9]{2}\s+[0-9:.]+)\s*,\s*"
    r"(?P<x_km>[+-]?[0-9.]+E[+-][0-9]+)\s*,\s*"
    r"(?P<y_km>[+-]?[0-9.]+E[+-][0-9]+)\s*,\s*"
    r"(?P<z_km>[+-]?[0-9.]+E[+-][0-9]+)\s*,\s*"
    r"(?P<vx_km_s>[+-]?[0-9.]+E[+-][0-9]+)\s*,\s*"
    r"(?P<vy_km_s>[+-]?[0-9.]+E[+-][0-9]+)\s*,\s*"
    r"(?P<vz_km_s>[+-]?[0-9.]+E[+-][0-9]+)\s*,?\s*$"
)


def raw_jpl_horizons_table_name() -> str:
    return bronze_table_name(RAW_JPL_HORIZONS_TABLE)


def jpl_ephemeris_vectors_table_name() -> str:
    return silver_table_name(JPL_EPHEMERIS_VECTORS_TABLE)


def parse_horizons_timestamp(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y-%b-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)


def extract_vector_block(response_body: str) -> str:
    try:
        parsed_body = json.loads(response_body)
        response_text = parsed_body.get("result", response_body)
    except json.JSONDecodeError:
        response_text = response_body

    start_marker = "$$SOE"
    end_marker = "$$EOE"

    start = response_text.find(start_marker)
    end = response_text.find(end_marker)
    if start == -1 or end == -1 or end <= start:
        return ""

    return response_text[start + len(start_marker) : end]


def parse_horizons_vectors(
    *,
    response_body: str,
    source_ingestion_run_id: str,
    source_response_hash: str,
    mission_name: str,
    bronze_ingested_at: datetime,
    processed_at: datetime | None = None,
) -> list[dict[str, Any]]:
    processed_at = processed_at or datetime.now(timezone.utc)
    rows = []

    for line in extract_vector_block(response_body).splitlines():
        match = VECTOR_ROW_PATTERN.match(line)
        if not match:
            continue

        values = match.groupdict()
        rows.append(
            {
                "source_ingestion_run_id": source_ingestion_run_id,
                "source_response_hash": source_response_hash,
                "target_body": TARGET_BODY,
                "center": CENTER,
                "observation_julian_date": float(values["julian_date"]),
                "observation_timestamp_utc": parse_horizons_timestamp(values["timestamp"]),
                "x_km": float(values["x_km"]),
                "y_km": float(values["y_km"]),
                "z_km": float(values["z_km"]),
                "vx_km_s": float(values["vx_km_s"]),
                "vy_km_s": float(values["vy_km_s"]),
                "vz_km_s": float(values["vz_km_s"]),
                "mission_name": mission_name,
                "bronze_ingested_at": bronze_ingested_at,
                "processed_at": processed_at,
            }
        )

    return rows


def silver_schema():
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("source_ingestion_run_id", StringType(), False),
            StructField("source_response_hash", StringType(), False),
            StructField("target_body", StringType(), False),
            StructField("center", StringType(), False),
            StructField("observation_julian_date", DoubleType(), False),
            StructField("observation_timestamp_utc", TimestampType(), False),
            StructField("x_km", DoubleType(), False),
            StructField("y_km", DoubleType(), False),
            StructField("z_km", DoubleType(), False),
            StructField("vx_km_s", DoubleType(), False),
            StructField("vy_km_s", DoubleType(), False),
            StructField("vz_km_s", DoubleType(), False),
            StructField("mission_name", StringType(), False),
            StructField("bronze_ingested_at", TimestampType(), False),
            StructField("processed_at", TimestampType(), False),
        ]
    )


def create_silver_dataframe(spark, records: list[dict[str, Any]]):
    rows = [[record[column] for column in JPL_EPHEMERIS_SILVER_COLUMNS] for record in records]
    return spark.createDataFrame(rows, silver_schema())


def ensure_silver_table(spark, target_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
          source_ingestion_run_id STRING COMMENT 'Bronze ingestion run ID that produced this vector',
          source_response_hash STRING COMMENT 'SHA-256 hash of the source Bronze response body',
          target_body STRING COMMENT 'Ephemeris target body',
          center STRING COMMENT 'Ephemeris coordinate center',
          observation_julian_date DOUBLE COMMENT 'Julian date from JPL Horizons',
          observation_timestamp_utc TIMESTAMP COMMENT 'UTC observation timestamp',
          x_km DOUBLE COMMENT 'X position in kilometers',
          y_km DOUBLE COMMENT 'Y position in kilometers',
          z_km DOUBLE COMMENT 'Z position in kilometers',
          vx_km_s DOUBLE COMMENT 'X velocity in kilometers per second',
          vy_km_s DOUBLE COMMENT 'Y velocity in kilometers per second',
          vz_km_s DOUBLE COMMENT 'Z velocity in kilometers per second',
          mission_name STRING COMMENT 'Mission name associated with the configured window',
          bronze_ingested_at TIMESTAMP COMMENT 'Timestamp when the source Bronze response was ingested',
          processed_at TIMESTAMP COMMENT 'Timestamp when this Silver row was produced'
        )
        USING DELTA
        COMMENT 'Silver normalized JPL Horizons ephemeris vectors for Project Orion'
        TBLPROPERTIES (
          'project' = 'orion',
          'quality' = 'silver',
          'source' = 'nasa_jpl_horizons'
        )
        """
    )


def align_silver_dataframe(silver_df):
    from pyspark.sql import functions as F

    return silver_df.select(
        *[
            F.col(column_name).cast(column_type).alias(column_name)
            for column_name, column_type in JPL_EPHEMERIS_SILVER_COLUMN_TYPES
        ]
    )


def insert_silver_dataframe(spark, aligned_df, target_table: str) -> None:
    temp_view = f"tmp_jpl_ephemeris_vectors_{uuid.uuid4().hex}"
    column_list = ", ".join(f"`{column}`" for column in JPL_EPHEMERIS_SILVER_COLUMNS)

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


def transform_jpl_ephemeris(
    spark,
    *,
    source_table: str | None = None,
    target_table: str | None = None,
) -> dict[str, Any]:
    source_table = source_table or raw_jpl_horizons_table_name()
    target_table = target_table or jpl_ephemeris_vectors_table_name()

    bronze_rows = spark.sql(
        f"""
        SELECT
            ingestion_run_id,
            response_hash,
            response_body,
            mission_name,
            ingested_at
        FROM {source_table}
        WHERE response_status_code = 200
          AND response_body LIKE '%$$SOE%'
          AND response_body NOT LIKE '%INPUT ERROR%'
        """
    ).collect()

    records: list[dict[str, Any]] = []
    for row in bronze_rows:
        records.extend(
            parse_horizons_vectors(
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
