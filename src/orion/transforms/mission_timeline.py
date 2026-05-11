from __future__ import annotations

from typing import Any

from src.orion.config import (
    CONFIG,
    GOLD_MISSION_TIMELINE_COLUMNS,
    TELEMETRY_SPACE_WEATHER_SILVER_COLUMNS,
    gold_table_name,
    silver_table_name,
)

JPL_EPHEMERIS_VECTORS_TABLE = "jpl_ephemeris_vectors"
DONKI_SPACE_WEATHER_EVENTS_TABLE = "donki_space_weather_events"
TELEMETRY_SPACE_WEATHER_HOURLY_TABLE = "telemetry_space_weather_hourly"
MISSION_TIMELINE_TABLE = "mission_timeline"
PBI_MISSION_TIMELINE_TABLE = "pbi_mission_timeline"
PBI_SPACE_WEATHER_EVENTS_TABLE = "pbi_space_weather_events"
PBI_ACTIVITY_SUMMARY_TABLE = "pbi_activity_summary"
DEFAULT_JOIN_WINDOW_HOURS = 6


def jpl_ephemeris_vectors_table_name() -> str:
    return silver_table_name(JPL_EPHEMERIS_VECTORS_TABLE)


def donki_space_weather_events_table_name() -> str:
    return silver_table_name(DONKI_SPACE_WEATHER_EVENTS_TABLE)


def telemetry_space_weather_hourly_table_name() -> str:
    return silver_table_name(TELEMETRY_SPACE_WEATHER_HOURLY_TABLE)


def mission_timeline_table_name() -> str:
    return gold_table_name(MISSION_TIMELINE_TABLE)


def pbi_mission_timeline_table_name() -> str:
    return gold_table_name(PBI_MISSION_TIMELINE_TABLE)


def pbi_space_weather_events_table_name() -> str:
    return gold_table_name(PBI_SPACE_WEATHER_EVENTS_TABLE)


def pbi_activity_summary_table_name() -> str:
    return gold_table_name(PBI_ACTIVITY_SUMMARY_TABLE)


def create_silver_join_table_sql(target_table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      observation_timestamp_utc TIMESTAMP COMMENT 'UTC telemetry observation timestamp',
      target_body STRING COMMENT 'Ephemeris target body',
      center STRING COMMENT 'Ephemeris coordinate center',
      x_km DOUBLE COMMENT 'X position in kilometers',
      y_km DOUBLE COMMENT 'Y position in kilometers',
      z_km DOUBLE COMMENT 'Z position in kilometers',
      vx_km_s DOUBLE COMMENT 'X velocity in kilometers per second',
      vy_km_s DOUBLE COMMENT 'Y velocity in kilometers per second',
      vz_km_s DOUBLE COMMENT 'Z velocity in kilometers per second',
      mission_name STRING COMMENT 'Mission name',
      nearby_space_weather_event_count BIGINT COMMENT 'DONKI events within the configured join window',
      nearby_flare_event_count BIGINT COMMENT 'Solar flare events within the configured join window',
      nearby_ips_event_count BIGINT COMMENT 'Interplanetary shock events within the configured join window',
      nearby_event_types_csv STRING COMMENT 'Distinct nearby DONKI event types',
      nearby_flare_classes_csv STRING COMMENT 'Distinct nearby flare classes',
      has_nearby_m_class_flare BOOLEAN COMMENT 'True when an M-class flare is within the join window',
      has_nearby_x_class_flare BOOLEAN COMMENT 'True when an X-class flare is within the join window',
      join_window_hours INT COMMENT 'Join window in hours before and after telemetry observation',
      processed_at TIMESTAMP COMMENT 'Timestamp when this Silver row was produced'
    )
    USING DELTA
    COMMENT 'Silver hourly telemetry enriched with nearby DONKI space-weather events'
    TBLPROPERTIES (
      'project' = 'orion',
      'quality' = 'silver'
    )
    """


def create_gold_timeline_table_sql(target_table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      mission_name STRING COMMENT 'Mission name',
      observation_timestamp_utc TIMESTAMP COMMENT 'UTC timeline timestamp',
      mission_elapsed_hours BIGINT COMMENT 'Elapsed hours since the first mission timeline row',
      target_body STRING COMMENT 'Ephemeris target body',
      center STRING COMMENT 'Ephemeris coordinate center',
      earth_moon_distance_km DOUBLE COMMENT 'Distance from Earth center to Moon center in kilometers',
      moon_speed_km_s DOUBLE COMMENT 'Moon speed relative to Earth in kilometers per second',
      x_km DOUBLE COMMENT 'X position in kilometers',
      y_km DOUBLE COMMENT 'Y position in kilometers',
      z_km DOUBLE COMMENT 'Z position in kilometers',
      vx_km_s DOUBLE COMMENT 'X velocity in kilometers per second',
      vy_km_s DOUBLE COMMENT 'Y velocity in kilometers per second',
      vz_km_s DOUBLE COMMENT 'Z velocity in kilometers per second',
      nearby_space_weather_event_count BIGINT COMMENT 'Nearby DONKI event count',
      nearby_event_types_csv STRING COMMENT 'Distinct nearby DONKI event types',
      nearby_flare_classes_csv STRING COMMENT 'Distinct nearby flare classes',
      has_nearby_m_class_flare BOOLEAN COMMENT 'M-class flare nearby flag',
      has_nearby_x_class_flare BOOLEAN COMMENT 'X-class flare nearby flag',
      space_weather_activity_level STRING COMMENT 'Simple serving label for nearby activity',
      processed_at TIMESTAMP COMMENT 'Timestamp when this Gold row was produced'
    )
    USING DELTA
    COMMENT 'Gold mission timeline for Project Orion Power BI serving'
    TBLPROPERTIES (
      'project' = 'orion',
      'quality' = 'gold'
    )
    """


def build_silver_join_sql(
    *,
    source_ephemeris_table: str,
    source_space_weather_table: str,
    target_table: str,
    join_window_hours: int = DEFAULT_JOIN_WINDOW_HOURS,
) -> str:
    column_list = ", ".join(TELEMETRY_SPACE_WEATHER_SILVER_COLUMNS)
    return f"""
    INSERT INTO {target_table} ({column_list})
    SELECT
      e.observation_timestamp_utc,
      e.target_body,
      e.center,
      e.x_km,
      e.y_km,
      e.z_km,
      e.vx_km_s,
      e.vy_km_s,
      e.vz_km_s,
      e.mission_name,
      COUNT(w.event_id) AS nearby_space_weather_event_count,
      SUM(CASE WHEN w.event_type = 'FLR' THEN 1 ELSE 0 END) AS nearby_flare_event_count,
      SUM(CASE WHEN w.event_type = 'IPS' THEN 1 ELSE 0 END) AS nearby_ips_event_count,
      CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(w.event_type))) AS nearby_event_types_csv,
      CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(w.class_type))) AS nearby_flare_classes_csv,
      MAX(CASE WHEN w.class_type LIKE 'M%' THEN true ELSE false END) AS has_nearby_m_class_flare,
      MAX(CASE WHEN w.class_type LIKE 'X%' THEN true ELSE false END) AS has_nearby_x_class_flare,
      {join_window_hours} AS join_window_hours,
      CURRENT_TIMESTAMP() AS processed_at
    FROM {source_ephemeris_table} e
    LEFT JOIN {source_space_weather_table} w
      ON w.event_time BETWEEN e.observation_timestamp_utc - INTERVAL {join_window_hours} HOURS
                          AND e.observation_timestamp_utc + INTERVAL {join_window_hours} HOURS
    GROUP BY
      e.observation_timestamp_utc,
      e.target_body,
      e.center,
      e.x_km,
      e.y_km,
      e.z_km,
      e.vx_km_s,
      e.vy_km_s,
      e.vz_km_s,
      e.mission_name
    """


def build_gold_timeline_sql(*, source_table: str, target_table: str) -> str:
    column_list = ", ".join(GOLD_MISSION_TIMELINE_COLUMNS)
    return f"""
    INSERT INTO {target_table} ({column_list})
    WITH enriched AS (
      SELECT
        *,
        MIN(observation_timestamp_utc) OVER () AS mission_start_timestamp
      FROM {source_table}
    )
    SELECT
      mission_name,
      observation_timestamp_utc,
      CAST((UNIX_TIMESTAMP(observation_timestamp_utc) - UNIX_TIMESTAMP(mission_start_timestamp)) / 3600 AS BIGINT)
        AS mission_elapsed_hours,
      target_body,
      center,
      SQRT(POWER(x_km, 2) + POWER(y_km, 2) + POWER(z_km, 2)) AS earth_moon_distance_km,
      SQRT(POWER(vx_km_s, 2) + POWER(vy_km_s, 2) + POWER(vz_km_s, 2)) AS moon_speed_km_s,
      x_km,
      y_km,
      z_km,
      vx_km_s,
      vy_km_s,
      vz_km_s,
      nearby_space_weather_event_count,
      nearby_event_types_csv,
      nearby_flare_classes_csv,
      has_nearby_m_class_flare,
      has_nearby_x_class_flare,
      CASE
        WHEN has_nearby_x_class_flare THEN 'high'
        WHEN has_nearby_m_class_flare OR nearby_space_weather_event_count >= 3 THEN 'moderate'
        WHEN nearby_space_weather_event_count > 0 THEN 'low'
        ELSE 'none'
      END AS space_weather_activity_level,
      CURRENT_TIMESTAMP() AS processed_at
    FROM enriched
    """


def rebuild_telemetry_space_weather_hourly(
    spark,
    *,
    source_ephemeris_table: str | None = None,
    source_space_weather_table: str | None = None,
    target_table: str | None = None,
    join_window_hours: int = DEFAULT_JOIN_WINDOW_HOURS,
) -> dict[str, Any]:
    source_ephemeris_table = source_ephemeris_table or jpl_ephemeris_vectors_table_name()
    source_space_weather_table = source_space_weather_table or donki_space_weather_events_table_name()
    target_table = target_table or telemetry_space_weather_hourly_table_name()

    spark.sql(create_silver_join_table_sql(target_table))
    spark.sql(f"DELETE FROM {target_table} WHERE mission_name = '{CONFIG.mission_name}'")
    spark.sql(
        build_silver_join_sql(
            source_ephemeris_table=source_ephemeris_table,
            source_space_weather_table=source_space_weather_table,
            target_table=target_table,
            join_window_hours=join_window_hours,
        )
    )

    rows_written = spark.table(target_table).where(f"mission_name = '{CONFIG.mission_name}'").count()
    return {
        "source_ephemeris_table": source_ephemeris_table,
        "source_space_weather_table": source_space_weather_table,
        "target_table": target_table,
        "join_window_hours": join_window_hours,
        "rows_written": rows_written,
    }


def rebuild_mission_timeline(
    spark,
    *,
    source_table: str | None = None,
    target_table: str | None = None,
) -> dict[str, Any]:
    source_table = source_table or telemetry_space_weather_hourly_table_name()
    target_table = target_table or mission_timeline_table_name()

    spark.sql(create_gold_timeline_table_sql(target_table))
    spark.sql(f"DELETE FROM {target_table} WHERE mission_name = '{CONFIG.mission_name}'")
    spark.sql(build_gold_timeline_sql(source_table=source_table, target_table=target_table))

    rows_written = spark.table(target_table).where(f"mission_name = '{CONFIG.mission_name}'").count()
    return {
        "source_table": source_table,
        "target_table": target_table,
        "rows_written": rows_written,
    }


def build_pbi_mission_timeline_sql(*, source_table: str, target_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE {target_table}
    USING DELTA
    COMMENT 'Power BI-ready hourly mission timeline fact table'
    TBLPROPERTIES (
      'project' = 'orion',
      'quality' = 'gold',
      'serving' = 'powerbi'
    )
    AS
    SELECT
      mission_name,
      observation_timestamp_utc,
      DATE(observation_timestamp_utc) AS observation_date,
      HOUR(observation_timestamp_utc) AS observation_hour_utc,
      mission_elapsed_hours,
      target_body,
      center,
      earth_moon_distance_km,
      moon_speed_km_s,
      x_km,
      y_km,
      z_km,
      vx_km_s,
      vy_km_s,
      vz_km_s,
      nearby_space_weather_event_count,
      nearby_event_types_csv,
      nearby_flare_classes_csv,
      has_nearby_m_class_flare,
      has_nearby_x_class_flare,
      space_weather_activity_level,
      CASE space_weather_activity_level
        WHEN 'high' THEN 3
        WHEN 'moderate' THEN 2
        WHEN 'low' THEN 1
        ELSE 0
      END AS space_weather_activity_score,
      processed_at
    FROM {source_table}
    """


def build_pbi_space_weather_events_sql(*, source_table: str, target_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE {target_table}
    USING DELTA
    COMMENT 'Power BI-ready DONKI space-weather event detail table'
    TBLPROPERTIES (
      'project' = 'orion',
      'quality' = 'gold',
      'serving' = 'powerbi'
    )
    AS
    SELECT
      mission_name,
      event_type,
      event_id,
      event_time,
      DATE(event_time) AS event_date,
      HOUR(event_time) AS event_hour_utc,
      catalog,
      class_type,
      location,
      source_location,
      active_region_number,
      version_id,
      link,
      instruments_json,
      linked_events_json,
      sent_notifications_json,
      note,
      source_ingestion_run_id,
      source_response_hash,
      bronze_ingested_at,
      processed_at
    FROM {source_table}
    """


def build_pbi_activity_summary_sql(*, source_table: str, target_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE {target_table}
    USING DELTA
    COMMENT 'Power BI-ready mission activity summary by date and activity level'
    TBLPROPERTIES (
      'project' = 'orion',
      'quality' = 'gold',
      'serving' = 'powerbi'
    )
    AS
    SELECT
      mission_name,
      observation_date,
      space_weather_activity_level,
      COUNT(*) AS timeline_hours,
      SUM(nearby_space_weather_event_count) AS nearby_space_weather_event_count,
      AVG(earth_moon_distance_km) AS avg_earth_moon_distance_km,
      MIN(earth_moon_distance_km) AS min_earth_moon_distance_km,
      MAX(earth_moon_distance_km) AS max_earth_moon_distance_km,
      AVG(moon_speed_km_s) AS avg_moon_speed_km_s,
      MAX(space_weather_activity_score) AS max_space_weather_activity_score
    FROM {source_table}
    GROUP BY
      mission_name,
      observation_date,
      space_weather_activity_level
    """


def rebuild_powerbi_tables(
    spark,
    *,
    mission_timeline_source_table: str | None = None,
    space_weather_source_table: str | None = None,
    timeline_target_table: str | None = None,
    events_target_table: str | None = None,
    summary_target_table: str | None = None,
) -> dict[str, Any]:
    mission_timeline_source_table = mission_timeline_source_table or mission_timeline_table_name()
    space_weather_source_table = space_weather_source_table or donki_space_weather_events_table_name()
    timeline_target_table = timeline_target_table or pbi_mission_timeline_table_name()
    events_target_table = events_target_table or pbi_space_weather_events_table_name()
    summary_target_table = summary_target_table or pbi_activity_summary_table_name()

    spark.sql(
        build_pbi_mission_timeline_sql(
            source_table=mission_timeline_source_table,
            target_table=timeline_target_table,
        )
    )
    spark.sql(
        build_pbi_space_weather_events_sql(
            source_table=space_weather_source_table,
            target_table=events_target_table,
        )
    )
    spark.sql(
        build_pbi_activity_summary_sql(
            source_table=timeline_target_table,
            target_table=summary_target_table,
        )
    )

    return {
        "timeline_target_table": timeline_target_table,
        "events_target_table": events_target_table,
        "summary_target_table": summary_target_table,
        "timeline_rows": spark.table(timeline_target_table).count(),
        "event_rows": spark.table(events_target_table).count(),
        "summary_rows": spark.table(summary_target_table).count(),
    }
