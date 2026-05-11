TABLE_NAME = "dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons"
DISPLAY_COLUMNS = [
    "ingestion_run_id",
    "source_system",
    "source_endpoint",
    "response_status_code",
    "response_hash",
    "ingested_at",
    "ingested_date",
    "mission_name",
]


def is_missing_table_error(message: str) -> bool:
    missing_table_markers = (
        "TABLE_OR_VIEW_NOT_FOUND",
        "NoSuchTableException",
        "DELTA_TABLE_NOT_FOUND",
        "not found",
    )
    return any(marker in message for marker in missing_table_markers)


def main() -> None:
    try:
        from databricks.connect import DatabricksSession
    except ModuleNotFoundError:
        print("Databricks Connect is not installed in the selected Python environment.")
        print("In Cursor, select .venv/bin/python, then use the Databricks extension Python Environment checklist.")
        return

    try:
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
    except Exception as exc:
        print("Could not create a Databricks Connect Spark session.")
        print("Check the Databricks extension target, auth profile, compute, and Python environment setup.")
        print(f"{exc.__class__.__name__}: {exc}")
        return

    df = spark.sql("""
    SELECT
      current_catalog() AS current_catalog,
      current_user() AS current_user,
      current_timestamp() AS tested_at
    """)

    df.show(truncate=False)

    try:
        table_df = spark.table(TABLE_NAME)
        existing_columns = [column for column in DISPLAY_COLUMNS if column in table_df.columns]

        print(f"Read table: {TABLE_NAME}")
        if existing_columns:
            table_df.select(*existing_columns).show(10, truncate=False)
        else:
            print(
                "Table exists, but none of the expected display columns were found. "
                f"Available columns: {table_df.columns}"
            )
    except Exception as exc:
        message = str(exc)

        if is_missing_table_error(message):
            print(f"Table not found yet: {TABLE_NAME}")
            print("Create or ingest the bronze JPL Horizons table before validating sample rows.")
        else:
            print(f"Could not read table: {TABLE_NAME}")
            print(f"{exc.__class__.__name__}: {message}")
            raise


if __name__ == "__main__":
    main()
