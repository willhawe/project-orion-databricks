# Local Databricks Development

Use this workflow to run Project Orion code from Cursor against the connected Azure Databricks workspace.

## Python Environment

Create a Python 3.11 virtual environment from the repo root:

```bash
python3.11 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install local development requirements:

```bash
pip install -r requirements-dev.txt
```

In Cursor, select this interpreter:

```text
/Users/willhawe/project-orion-databricks/.venv/bin/python
```

## Databricks Extension Workflow

1. Open the Databricks extension in Cursor.
2. Confirm the target is `dev` and the auth profile is `project_orion_databricks`.
3. Use the extension's Python Environment checklist to install and configure Databricks Connect after the `.venv` interpreter is selected.
4. Start remote folder sync from the extension.
5. Run `scratch/databricks_connection_test.py` with the Databricks extension.

For serverless compute, use **Run File as Workflow** for full file execution. Direct **Upload and Run File** can require a cluster instead of serverless compute.

Remote folder sync is one-way from this local repo to the Databricks workspace. Do not edit the synced remote workspace copy directly.
