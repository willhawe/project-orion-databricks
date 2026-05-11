from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable

from src.orion.config import (
    CONFIG,
    JPL_HORIZONS_BRONZE_COLUMNS,
    JPL_HORIZONS_BRONZE_COLUMN_TYPES,
    bronze_table_name,
)

SOURCE_SYSTEM = "nasa_jpl_horizons"
SOURCE_ENDPOINT = "horizons_vectors"
RAW_JPL_HORIZONS_TABLE = "raw_jpl_horizons"


def raw_jpl_horizons_table_name() -> str:
    return bronze_table_name(RAW_JPL_HORIZONS_TABLE)


def build_request_params() -> dict[str, str]:
    return {
        "format": "json",
        "COMMAND": "301",
        "OBJ_DATA": "YES",
        "MAKE_EPHEM": "YES",
        "EPHEM_TYPE": "VECTORS",
        "CENTER": "500@399",
        "START_TIME": CONFIG.mission_start_date,
        "STOP_TIME": CONFIG.mission_end_date,
        "STEP_SIZE": "1 h",
        "VEC_TABLE": "2",
        "CSV_FORMAT": "YES",
    }


def build_bronze_record(
    *,
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
        "source_endpoint": SOURCE_ENDPOINT,
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


def create_bronze_dataframe(spark, record: dict[str, Any]):
    return spark.createDataFrame([[record[column] for column in JPL_HORIZONS_BRONZE_COLUMNS]], bronze_schema())


def response_hash_exists(spark, target_table: str, response_hash: str) -> bool:
    from pyspark.sql import functions as F

    return (
        spark.table(target_table)
        .where(F.col("response_hash") == response_hash)
        .limit(1)
        .count()
        > 0
    )


def align_bronze_dataframe(bronze_df):
    from pyspark.sql import functions as F

    return bronze_df.select(
        *[
            F.col(column_name).cast(column_type).alias(column_name)
            for column_name, column_type in JPL_HORIZONS_BRONZE_COLUMN_TYPES
        ]
    )


def insert_bronze_dataframe(spark, aligned_df, target_table: str) -> None:
    temp_view = f"tmp_raw_jpl_horizons_{uuid.uuid4().hex}"
    column_list = ", ".join(f"`{column}`" for column in JPL_HORIZONS_BRONZE_COLUMNS)

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


def append_if_new_response(spark, bronze_df, target_table: str) -> int:
    response_hash = bronze_df.select("response_hash").first()["response_hash"]
    if response_hash_exists(spark, target_table, response_hash):
        return 0

    aligned_df = align_bronze_dataframe(bronze_df)
    records_to_write = aligned_df.count()
    insert_bronze_dataframe(spark, aligned_df, target_table)
    return records_to_write


def default_http_get(url: str, *, params: dict[str, str], timeout: int):
    import requests

    return requests.get(url, params=params, timeout=timeout)


def ingest_jpl_horizons(
    spark,
    *,
    http_get: Callable[..., Any] = default_http_get,
    target_table: str | None = None,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    request_params = build_request_params()
    response = http_get(CONFIG.jpl_horizons_base_url, params=request_params, timeout=timeout_seconds)

    record = build_bronze_record(
        response_body=response.text,
        response_status_code=response.status_code,
        request_url=response.url,
        request_params=request_params,
    )
    bronze_df = create_bronze_dataframe(spark, record)
    table_name = target_table or raw_jpl_horizons_table_name()
    records_written = append_if_new_response(spark, bronze_df, table_name)

    return {
        "ingestion_run_id": record["ingestion_run_id"],
        "response_status_code": record["response_status_code"],
        "response_hash": record["response_hash"],
        "records_written": records_written,
        "target_table": table_name,
        "request_url": record["request_url"],
    }
