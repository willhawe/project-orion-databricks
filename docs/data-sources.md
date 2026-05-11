# Data Sources

## NASA/JPL Horizons

Base URL:

```text
https://ssd.jpl.nasa.gov/api/horizons.api
```

Purpose:

- Retrieve Moon geocentric vector data for the Artemis II historical mission window.
- Store raw response in Bronze.
- Parse `$$SOE` / `$$EOE` vector rows into Silver.

Current request shape:

```text
COMMAND=301
CENTER=500@399
EPHEM_TYPE=VECTORS
STEP_SIZE='1 h'
START_TIME=2026-04-01
STOP_TIME=2026-04-10
```

## NASA DONKI

Base URL:

```text
https://api.nasa.gov/DONKI
```

Current endpoints:

```text
CME
FLR
GST
IPS
SEP
```

The Bronze ingestion uses `NASA_API_KEY` if present, otherwise `DEMO_KEY`. Successful JSON array responses are parsed into Silver; non-200 responses remain in Bronze only.
