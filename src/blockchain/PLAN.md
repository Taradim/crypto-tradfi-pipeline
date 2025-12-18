# PLAN — Dune → dlt → DuckDB (v1 API Dune direct, v2 Spice)

## Goal

Build a small, production-grade ingestion module that:

- Executes one or more Dune queries
- Loads results into **DuckDB** using **dlt**
- Supports both:
  - **Full refresh / merge** loads
  - **Incremental** loads (cursor-based), as described in the Medium article

This repo starts with **v1 using the Dune API directly** (no Spice), then upgrades to **v2 using Spice** with minimal code changes by keeping stable interfaces.

## Non-goals (for now)

- Postgres destination (you said you will use it for something else)
- Full-featured CLI with many options (we will keep it minimal and extend later)
- Orchestration (Airflow) — we will keep the code Airflow-friendly, but no DAG here yet

## Target design (high-level)

### Key components

- **Config loader**: loads query specs from `.dlt/config.toml`
- **Dune client (v1)**: executes query + polls for completion + fetches results
- **dlt source factory**: turns each query spec into a `dlt.resource`
- **Pipeline runner**: runs `dlt.pipeline(... destination="duckdb" ...)` and loads into a dataset

### Why this design

- Keeps business logic (query specs + incremental semantics) **data-driven** via TOML
- Separates concerns:
  - API I/O and retries in a client
  - dlt concerns in a thin adapter layer
- Makes the Spice upgrade a **drop-in replacement** for the client

## Configuration

### `.dlt/config.toml` (query specifications)

The config follows the article’s pattern:

- Top-level namespace: `dune_queries`
- Each entry defines one query resource/table.

Example:

```toml
########## Example 1: DEX Volume ##########
[dune_queries.dex_volume]
query = "https://dune.com/queries/4388"
primary_key = ["project", "_col1"]
write_disposition = "merge"

##### Example 2: DEX Volume With Incremental #####
[dune_queries.dex_volume_incremental]
query = "https://dune.com/queries/4778954"
primary_key = ["project", "date"]
write_disposition = "merge"
replication_key = "date"
starting_replication_value = "2025-01-01"
```

Field semantics:

- `query`: Dune query URL or query id (we will support URL-first)
- `primary_key`: string or list of strings
- `write_disposition`: `"merge" | "append" | "replace"`
- `replication_key`: column used as incremental cursor field (optional)
- `starting_replication_value`: initial cursor value (required if `replication_key` is set)

### `.dlt/secrets.toml` (secrets)

We will store the Dune API key using dlt conventions:

```toml
[dune_source]
api_key = "YOUR_DUNE_API_KEY"
```

Notes:

- Keep this file out of version control (typical).
- dlt can also read from env vars; we’ll start with secrets.toml because it matches the article.

## dlt conventions

### Pipeline config

- `pipeline_name`: `dune_source`
- `destination`: `duckdb`
- `dataset_name`: `dune_queries`

Effect:

- Each TOML entry becomes a DuckDB table inside the `dune_queries` dataset/schema.

## Implementation plan (step-by-step)

### Step 0 — Plan document (this file)

Done once: align scope + config + upgrade path.

### Step 1 — Config model + loader

Create `src/blockchain/dune_config.py`:

- Load TOML from `.dlt/config.toml`
- Validate structure and required fields
- Normalize `primary_key` to `list[str] | None`
- Represent each query as a typed dataclass

### Step 2 — Dune API client (v1, direct HTTP)

Create `src/blockchain/dune_client.py`:

- Parse Dune query URL to query id
- Call Dune endpoints to:
  - Trigger execution (optionally with parameters)
  - Poll execution status with backoff
  - Fetch results (rows + columns)
- Handle:
  - timeouts
  - rate limiting (429)
  - transient errors (5xx)

### Step 3 — dlt resource/source factory

Create `src/blockchain/dune_dlt_source.py`:

- For each query spec:
  - Build a `dlt.resource` with:
    - `name` = config key (e.g. `dex_volume_incremental`)
    - `primary_key` from config
    - `write_disposition` from config
    - optional `dlt.sources.incremental(...)` when `replication_key` is set
  - Yield rows as `dict[str, Any]`
- Provide a `@dlt.source` that returns the list of resources

### Step 4 — Minimal runner / entrypoint

Create `src/blockchain/run_dune_to_duckdb.py` (or similar):

- Build pipeline
- Run it
- Log load info

## Upgrade plan — Replace Dune API client with Spice (v2)

Goal: the dlt adapter layer should not change.

- Replace `DuneClient` implementation behind an interface (e.g. `QueryExecutor`)
- Spice path:
  - `spice.query(query_or_sql, api_key=..., parameters=..., refresh=True, cache=False)`
  - Convert returned DataFrame to `to_dicts()` and yield

We will keep:

- Same TOML schema
- Same resource naming + dlt config
- Same incremental cursor logic

## Open questions (to decide later)

- Whether to store the TOML schema name as `dune_queries` only, or allow custom namespaces
- How to map Dune query parameters:
  - Explicit `parameters = { ... }` in TOML
  - Derive from replication cursor only (MVP)


