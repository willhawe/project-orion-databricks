from datetime import datetime, timezone
import json

from src.orion.config import JPL_EPHEMERIS_SILVER_COLUMNS, JPL_EPHEMERIS_SILVER_COLUMN_TYPES
from src.orion.transforms.jpl_ephemeris import (
    extract_vector_block,
    jpl_ephemeris_vectors_table_name,
    parse_horizons_timestamp,
    parse_horizons_vectors,
)

SAMPLE_RESPONSE = """
header text
$$SOE
2461131.500000000, A.D. 2026-Apr-01 00:00:00.0000, -3.894680455007774E+05,  1.289648175008370E+04, -1.194896872279250E+04, -7.035045575758354E-02, -1.006992176217861E+00, -8.744664167129373E-02,
2461131.541666667, A.D. 2026-Apr-01 01:00:00.0000, -3.897043122145533E+05,  9.270735963281131E+03, -1.226324187214790E+04, -6.090966194446214E-02, -1.007296128276940E+00, -8.714831598150247E-02,
$$EOE
footer text
"""


def test_extract_vector_block_returns_only_ephemeris_rows():
    block = extract_vector_block(SAMPLE_RESPONSE)

    assert "$$SOE" not in block
    assert "$$EOE" not in block
    assert "2461131.500000000" in block


def test_extract_vector_block_handles_horizons_json_response():
    block = extract_vector_block(json.dumps({"result": SAMPLE_RESPONSE}))

    assert "2461131.500000000" in block


def test_parse_horizons_timestamp_returns_utc_datetime():
    parsed = parse_horizons_timestamp("2026-Apr-01 00:00:00.0000")

    assert parsed == datetime(2026, 4, 1, tzinfo=timezone.utc)


def test_parse_horizons_vectors_returns_typed_rows():
    rows = parse_horizons_vectors(
        response_body=SAMPLE_RESPONSE,
        source_ingestion_run_id="run-1",
        source_response_hash="hash-1",
        mission_name="artemis_ii",
        bronze_ingested_at=datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
        processed_at=datetime(2026, 5, 11, 13, tzinfo=timezone.utc),
    )

    assert len(rows) == 2
    assert tuple(rows[0].keys()) == JPL_EPHEMERIS_SILVER_COLUMNS
    assert rows[0]["source_ingestion_run_id"] == "run-1"
    assert rows[0]["target_body"] == "moon"
    assert rows[0]["center"] == "earth"
    assert rows[0]["observation_timestamp_utc"] == datetime(2026, 4, 1, tzinfo=timezone.utc)
    assert rows[0]["x_km"] == -389468.0455007774
    assert rows[0]["vy_km_s"] == -1.006992176217861


def test_jpl_ephemeris_vectors_table_name():
    assert (
        jpl_ephemeris_vectors_table_name()
        == "dbw_orion_dev_uks_001.orion_silver.jpl_ephemeris_vectors"
    )


def test_jpl_ephemeris_silver_column_types_align_with_columns():
    assert tuple(column for column, _ in JPL_EPHEMERIS_SILVER_COLUMN_TYPES) == JPL_EPHEMERIS_SILVER_COLUMNS
