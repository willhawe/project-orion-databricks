# project-orion-databricks
Azure Databricks lakehouse project joining NASA Artemis II orbital telemetry with space weather events using Delta Lake, PySpark, Unity Catalog and Databricks Workflows.


## Current build status

- Azure Databricks workspace created: `dbw-orion-dev-uks-001`
- Unity Catalog workspace catalog confirmed: `dbw_orion_dev_uks_001`
- Medallion schemas created:
  - `dbw_orion_dev_uks_001.orion_bronze`
  - `dbw_orion_dev_uks_001.orion_silver`
  - `dbw_orion_dev_uks_001.orion_gold`
  - `dbw_orion_dev_uks_001.orion_audit`
  - `dbw_orion_dev_uks_001.orion_config`
- Smoke test table created:
  - `dbw_orion_dev_uks_001.orion_audit.workspace_smoke_test`

  For this development build, Project Orion uses the Databricks workspace catalog. 
In a production deployment, the same medallion schemas would sit inside a dedicated catalog such as `orion_prod`, backed by ADLS Gen2 managed storage.
