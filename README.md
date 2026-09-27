# CineInsight Movie Data Platform

Runnable local reference implementation for the supplied MovieLens 20M snapshot. It uses immutable CSV landing files, append-only SQLite Bronze, keyed Silver upserts, and dimensional Gold tables. SQLite is used because this environment has no Spark/Delta runtime; `docs/architecture.md` records the corresponding Delta Lake production design and limitations.

## Quick start

Use Python 3.10+. From this folder:

```powershell
python src/pipeline.py init
python src/pipeline.py run --chunk-size 250000
python src/pipeline.py report
```

The first run seeds event-time batches from the snapshot's rating/tag date range and simulates movie INSERT/UPDATE/soft DELETE changes. Re-running resumes from persisted watermarks and is idempotent. Use `--as-of YYYY-MM-DD` to limit a run; date handling follows source event windows. `--reset` is intentionally not provided; preserve landing and control state for replay.

Run `python src/ingestion/profiling.py` to stream-profile every supplied CSV and write `lakehouse/reports/profile.json`.

Outputs live under `control/` (SQLite database) and `lakehouse/` (immutable landing copies, quarantine CSV, reports). Dataset CSVs stay unchanged under `MovieLens/`. Paths and expected row counts are in `config/pipeline.json`.

## Deliverables

```text
config/                 runtime paths and ingestion settings
control/                SQLite control database
dags/                   Airflow DAG
docs/                   architecture and presentation guide
notebooks/              phase 1 profiling and phase 5 analytics
src/
  analytics/             HTML report export
  ingestion/             streaming source profiling
  pipeline.py            local Landing → Bronze → Silver → Gold pipeline
```

- `src/pipeline.py`: ingestion, reconciliation, incremental CDM, DQ/quarantine, SCD1/2/3, dimensional loads, analytics and URL construction.
- `src/ingestion/profiling.py`: streaming source profiler.
- `src/analytics/export_notebook_html.py`: HTML report exporter.
- `dags/cineinsight_pipeline.py`: Airflow DAG adapter with explicit stages/retries/SLA and backfill-compatible schedule.
- `docs/architecture.md` and `docs/architecture_vi.md`: design decisions, mappings, contracts, ERD, DQ rules, replay and operational notes in English and Vietnamese.
- `docs/huong_dan_trinh_bay.md`: Vietnamese stage-by-stage and code explanation for the project defense.
- `notebooks/phase1_profiling.ipynb`: profiling report and source risk review.
- `notebooks/phase5_analytics.ipynb` and `.html`: analytical findings. Regenerate the HTML after `pipeline.py run` with `python src/analytics/export_notebook_html.py` to include the latest Gold report tables.

SQLite implements transactions and UPSERT but is not a distributed lakehouse table format. For a production multi-node deployment, retain the contracts and replace storage adapters with Spark/Delta or Iceberg MERGE tables, object storage, and a catalog.
