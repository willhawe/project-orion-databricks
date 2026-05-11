from src.orion.config import GOLD_MISSION_TIMELINE_COLUMNS, TELEMETRY_SPACE_WEATHER_SILVER_COLUMNS
from src.orion.transforms.mission_timeline import (
    DEFAULT_JOIN_WINDOW_HOURS,
    build_gold_timeline_sql,
    build_pbi_activity_summary_sql,
    build_pbi_mission_timeline_sql,
    build_pbi_space_weather_events_sql,
    build_silver_join_sql,
    donki_space_weather_events_table_name,
    jpl_ephemeris_vectors_table_name,
    mission_timeline_table_name,
    pbi_activity_summary_table_name,
    pbi_mission_timeline_table_name,
    pbi_space_weather_events_table_name,
    telemetry_space_weather_hourly_table_name,
)


def test_table_name_helpers():
    assert (
        jpl_ephemeris_vectors_table_name()
        == "dbw_orion_dev_uks_001.orion_silver.jpl_ephemeris_vectors"
    )
    assert (
        donki_space_weather_events_table_name()
        == "dbw_orion_dev_uks_001.orion_silver.donki_space_weather_events"
    )
    assert (
        telemetry_space_weather_hourly_table_name()
        == "dbw_orion_dev_uks_001.orion_silver.telemetry_space_weather_hourly"
    )
    assert mission_timeline_table_name() == "dbw_orion_dev_uks_001.orion_gold.mission_timeline"
    assert pbi_mission_timeline_table_name() == "dbw_orion_dev_uks_001.orion_gold.pbi_mission_timeline"
    assert pbi_space_weather_events_table_name() == "dbw_orion_dev_uks_001.orion_gold.pbi_space_weather_events"
    assert pbi_activity_summary_table_name() == "dbw_orion_dev_uks_001.orion_gold.pbi_activity_summary"


def test_build_silver_join_sql_uses_configured_window_and_columns():
    sql = build_silver_join_sql(
        source_ephemeris_table="silver.ephemeris",
        source_space_weather_table="silver.events",
        target_table="silver.joined",
        join_window_hours=DEFAULT_JOIN_WINDOW_HOURS,
    )

    assert "INSERT INTO silver.joined" in sql
    assert "silver.ephemeris" in sql
    assert "silver.events" in sql
    assert "INTERVAL 6 HOURS" in sql
    assert "nearby_space_weather_event_count" in sql
    assert all(column in sql for column in TELEMETRY_SPACE_WEATHER_SILVER_COLUMNS)


def test_build_gold_timeline_sql_contains_serving_metrics():
    sql = build_gold_timeline_sql(source_table="silver.joined", target_table="gold.timeline")

    assert "INSERT INTO gold.timeline" in sql
    assert "silver.joined" in sql
    assert "earth_moon_distance_km" in sql
    assert "moon_speed_km_s" in sql
    assert "space_weather_activity_level" in sql
    assert all(column in sql for column in GOLD_MISSION_TIMELINE_COLUMNS)


def test_build_pbi_mission_timeline_sql_contains_powerbi_fields():
    sql = build_pbi_mission_timeline_sql(source_table="gold.timeline", target_table="gold.pbi_timeline")

    assert "CREATE OR REPLACE TABLE gold.pbi_timeline" in sql
    assert "gold.timeline" in sql
    assert "observation_date" in sql
    assert "space_weather_activity_score" in sql


def test_build_pbi_space_weather_events_sql_contains_event_fields():
    sql = build_pbi_space_weather_events_sql(source_table="silver.events", target_table="gold.pbi_events")

    assert "CREATE OR REPLACE TABLE gold.pbi_events" in sql
    assert "event_date" in sql
    assert "active_region_number" in sql
    assert "linked_events_json" in sql


def test_build_pbi_activity_summary_sql_contains_aggregate_metrics():
    sql = build_pbi_activity_summary_sql(source_table="gold.pbi_timeline", target_table="gold.pbi_summary")

    assert "CREATE OR REPLACE TABLE gold.pbi_summary" in sql
    assert "timeline_hours" in sql
    assert "avg_earth_moon_distance_km" in sql
    assert "GROUP BY" in sql
