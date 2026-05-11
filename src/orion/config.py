from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class OrionConfig:
    """
    Central configuration for Project Orion.

    This keeps catalog, schema, mission-window, and source-system settings
    out of individual notebooks.
    """

    catalog: str = "dbw_orion_dev_uks_001"

    bronze_schema: str = "orion_bronze"
    silver_schema: str = "orion_silver"
    gold_schema: str = "orion_gold"
    audit_schema: str = "orion_audit"
    config_schema: str = "orion_config"

    mission_name: str = "artemis_ii"
    mission_start_date: str = "2026-04-01"
    mission_end_date: str = "2026-04-10"

    jpl_horizons_base_url: str = "https://ssd.jpl.nasa.gov/api/horizons.api"
    donki_base_url: str = "https://api.nasa.gov/DONKI"


CONFIG: Final[OrionConfig] = OrionConfig()

JPL_HORIZONS_BRONZE_COLUMNS: Final[tuple[str, ...]] = (
    "ingestion_run_id",
    "source_system",
    "source_endpoint",
    "request_url",
    "request_params_json",
    "response_status_code",
    "response_body",
    "response_hash",
    "ingested_at",
    "ingested_date",
    "mission_name",
    "mission_start_date",
    "mission_end_date",
)

JPL_HORIZONS_BRONZE_COLUMN_TYPES: Final[tuple[tuple[str, str], ...]] = (
    ("ingestion_run_id", "string"),
    ("source_system", "string"),
    ("source_endpoint", "string"),
    ("request_url", "string"),
    ("request_params_json", "string"),
    ("response_status_code", "int"),
    ("response_body", "string"),
    ("response_hash", "string"),
    ("ingested_at", "timestamp"),
    ("ingested_date", "date"),
    ("mission_name", "string"),
    ("mission_start_date", "date"),
    ("mission_end_date", "date"),
)

DONKI_BRONZE_COLUMNS: Final[tuple[str, ...]] = (
    "ingestion_run_id",
    "source_system",
    "source_endpoint",
    "event_type",
    "request_url",
    "request_params_json",
    "response_status_code",
    "response_body",
    "response_hash",
    "ingested_at",
    "ingested_date",
    "mission_name",
    "mission_start_date",
    "mission_end_date",
)

DONKI_BRONZE_COLUMN_TYPES: Final[tuple[tuple[str, str], ...]] = (
    ("ingestion_run_id", "string"),
    ("source_system", "string"),
    ("source_endpoint", "string"),
    ("event_type", "string"),
    ("request_url", "string"),
    ("request_params_json", "string"),
    ("response_status_code", "int"),
    ("response_body", "string"),
    ("response_hash", "string"),
    ("ingested_at", "timestamp"),
    ("ingested_date", "date"),
    ("mission_name", "string"),
    ("mission_start_date", "date"),
    ("mission_end_date", "date"),
)

JPL_EPHEMERIS_SILVER_COLUMNS: Final[tuple[str, ...]] = (
    "source_ingestion_run_id",
    "source_response_hash",
    "target_body",
    "center",
    "observation_julian_date",
    "observation_timestamp_utc",
    "x_km",
    "y_km",
    "z_km",
    "vx_km_s",
    "vy_km_s",
    "vz_km_s",
    "mission_name",
    "bronze_ingested_at",
    "processed_at",
)

JPL_EPHEMERIS_SILVER_COLUMN_TYPES: Final[tuple[tuple[str, str], ...]] = (
    ("source_ingestion_run_id", "string"),
    ("source_response_hash", "string"),
    ("target_body", "string"),
    ("center", "string"),
    ("observation_julian_date", "double"),
    ("observation_timestamp_utc", "timestamp"),
    ("x_km", "double"),
    ("y_km", "double"),
    ("z_km", "double"),
    ("vx_km_s", "double"),
    ("vy_km_s", "double"),
    ("vz_km_s", "double"),
    ("mission_name", "string"),
    ("bronze_ingested_at", "timestamp"),
    ("processed_at", "timestamp"),
)

DONKI_SPACE_WEATHER_SILVER_COLUMNS: Final[tuple[str, ...]] = (
    "source_ingestion_run_id",
    "source_response_hash",
    "event_type",
    "event_id",
    "catalog",
    "event_time",
    "event_start_time",
    "event_peak_time",
    "event_end_time",
    "submission_time",
    "version_id",
    "location",
    "source_location",
    "active_region_number",
    "class_type",
    "link",
    "instruments_json",
    "linked_events_json",
    "sent_notifications_json",
    "note",
    "mission_name",
    "bronze_ingested_at",
    "processed_at",
)

DONKI_SPACE_WEATHER_SILVER_COLUMN_TYPES: Final[tuple[tuple[str, str], ...]] = (
    ("source_ingestion_run_id", "string"),
    ("source_response_hash", "string"),
    ("event_type", "string"),
    ("event_id", "string"),
    ("catalog", "string"),
    ("event_time", "timestamp"),
    ("event_start_time", "timestamp"),
    ("event_peak_time", "timestamp"),
    ("event_end_time", "timestamp"),
    ("submission_time", "timestamp"),
    ("version_id", "int"),
    ("location", "string"),
    ("source_location", "string"),
    ("active_region_number", "int"),
    ("class_type", "string"),
    ("link", "string"),
    ("instruments_json", "string"),
    ("linked_events_json", "string"),
    ("sent_notifications_json", "string"),
    ("note", "string"),
    ("mission_name", "string"),
    ("bronze_ingested_at", "timestamp"),
    ("processed_at", "timestamp"),
)

TELEMETRY_SPACE_WEATHER_SILVER_COLUMNS: Final[tuple[str, ...]] = (
    "observation_timestamp_utc",
    "target_body",
    "center",
    "x_km",
    "y_km",
    "z_km",
    "vx_km_s",
    "vy_km_s",
    "vz_km_s",
    "mission_name",
    "nearby_space_weather_event_count",
    "nearby_flare_event_count",
    "nearby_ips_event_count",
    "nearby_event_types_csv",
    "nearby_flare_classes_csv",
    "has_nearby_m_class_flare",
    "has_nearby_x_class_flare",
    "join_window_hours",
    "processed_at",
)

GOLD_MISSION_TIMELINE_COLUMNS: Final[tuple[str, ...]] = (
    "mission_name",
    "observation_timestamp_utc",
    "mission_elapsed_hours",
    "target_body",
    "center",
    "earth_moon_distance_km",
    "moon_speed_km_s",
    "x_km",
    "y_km",
    "z_km",
    "vx_km_s",
    "vy_km_s",
    "vz_km_s",
    "nearby_space_weather_event_count",
    "nearby_event_types_csv",
    "nearby_flare_classes_csv",
    "has_nearby_m_class_flare",
    "has_nearby_x_class_flare",
    "space_weather_activity_level",
    "processed_at",
)


def full_table_name(schema: str, table_name: str) -> str:
    """
    Build a fully-qualified Unity Catalog table name.

    Example:
        dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons
    """
    return f"{CONFIG.catalog}.{schema}.{table_name}"


def schema_name(layer: str) -> str:
    """
    Return the configured schema for a medallion/audit/config layer.
    """
    layer_map = {
        "bronze": CONFIG.bronze_schema,
        "silver": CONFIG.silver_schema,
        "gold": CONFIG.gold_schema,
        "audit": CONFIG.audit_schema,
        "config": CONFIG.config_schema,
    }

    if layer not in layer_map:
        valid_layers = ", ".join(layer_map.keys())
        raise ValueError(f"Unknown layer '{layer}'. Valid layers: {valid_layers}")

    return layer_map[layer]


def bronze_table_name(table_name: str) -> str:
    return full_table_name(CONFIG.bronze_schema, table_name)


def silver_table_name(table_name: str) -> str:
    return full_table_name(CONFIG.silver_schema, table_name)


def gold_table_name(table_name: str) -> str:
    return full_table_name(CONFIG.gold_schema, table_name)


def audit_table_name(table_name: str) -> str:
    return full_table_name(CONFIG.audit_schema, table_name)


def config_table_name(table_name: str) -> str:
    return full_table_name(CONFIG.config_schema, table_name)
