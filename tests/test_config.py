from src.orion.config import CONFIG, full_table_name, schema_name


def test_catalog_is_set():
    assert CONFIG.catalog == "dbw_orion_dev_uks_001"


def test_mission_window_is_set():
    assert CONFIG.mission_start_date == "2026-04-01"
    assert CONFIG.mission_end_date == "2026-04-10"


def test_schema_name_returns_expected_schema():
    assert schema_name("bronze") == "orion_bronze"
    assert schema_name("silver") == "orion_silver"
    assert schema_name("gold") == "orion_gold"


def test_full_table_name():
    assert (
        full_table_name("orion_bronze", "raw_jpl_horizons")
        == "dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons"
    )