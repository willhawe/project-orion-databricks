Power BI serving tables are built in the Databricks Gold schema by:

```text
notebooks/03_gold_serving/02_build_powerbi_tables.py.ipynb
```

Use these Unity Catalog tables as Power BI sources:

```text
dbw_orion_dev_uks_001.orion_gold.pbi_mission_timeline
dbw_orion_dev_uks_001.orion_gold.pbi_space_weather_events
dbw_orion_dev_uks_001.orion_gold.pbi_activity_summary
```

Suggested model:

- `pbi_mission_timeline`: hourly mission fact table.
- `pbi_space_weather_events`: DONKI event detail table.
- `pbi_activity_summary`: daily/activity-level aggregate for cards and summary visuals.

Useful first visuals:

- Line chart: `observation_timestamp_utc` vs `earth_moon_distance_km`.
- Line chart: `observation_timestamp_utc` vs `moon_speed_km_s`.
- Stacked column: `observation_date` by `space_weather_activity_level`.
- Table: `event_time`, `event_type`, `class_type`, `location`, `source_location`.
