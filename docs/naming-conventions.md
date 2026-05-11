# Naming Conventions

## Catalog And Schemas

Development catalog:

```text
dbw_orion_dev_uks_001
```

Schemas:

```text
orion_bronze
orion_silver
orion_gold
orion_audit
orion_config
```

## Table Names

Bronze raw landing tables use `raw_` prefixes:

```text
raw_jpl_horizons
raw_donki_events
```

Silver normalized tables use entity names:

```text
jpl_ephemeris_vectors
donki_space_weather_events
telemetry_space_weather_hourly
```

Gold serving tables use business/reporting names:

```text
mission_timeline
pbi_mission_timeline
pbi_space_weather_events
pbi_activity_summary
```

## Python Modules

Ingestion code lives in:

```text
src/orion/ingestion/
```

Transform and serving logic lives in:

```text
src/orion/transforms/
```

Local tests mirror module names under:

```text
tests/
```
