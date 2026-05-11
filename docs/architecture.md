# Architecture

Project Orion follows a Databricks medallion architecture.

## Bronze

Bronze stores raw source API responses with ingestion metadata, response hashes, source identifiers, mission window fields, and ingestion timestamps.

Tables:

```text
orion_bronze.raw_jpl_horizons
orion_bronze.raw_donki_events
```

Bronze intentionally preserves unsuccessful source responses, such as NASA API `503` payloads, because those are useful for operational observability and later audit.

## Silver

Silver normalizes successful Bronze payloads into typed analytical entities:

```text
orion_silver.jpl_ephemeris_vectors
orion_silver.donki_space_weather_events
orion_silver.telemetry_space_weather_hourly
```

`jpl_ephemeris_vectors` parses JPL Horizons `$$SOE` vector rows into typed position and velocity columns.

`donki_space_weather_events` flattens successful DONKI JSON arrays into common event fields and keeps nested arrays as JSON strings.

`telemetry_space_weather_hourly` enriches hourly Moon ephemeris vectors with DONKI events within a +/- 6 hour window.

## Gold

Gold serves curated reporting tables:

```text
orion_gold.mission_timeline
orion_gold.pbi_mission_timeline
orion_gold.pbi_space_weather_events
orion_gold.pbi_activity_summary
```

`mission_timeline` adds serving metrics such as mission elapsed hours, Earth-Moon distance, Moon speed, nearby activity flags, and an activity level.

The `pbi_*` tables are denormalized Power BI-friendly tables.

## Development Workflow

Code is authored locally in Cursor, synced to Databricks through the official Databricks extension, and executed with **Run File as Workflow** against serverless compute.
