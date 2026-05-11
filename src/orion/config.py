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