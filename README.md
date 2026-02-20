# training

Bronze ingestion scaffold for Databricks (Unity Catalog `dev` catalog).

## Structure
- `data/`: place raw source files/folders here locally.
- `pipelines/workspace_files/bronze_ingest.py`: notebook/script that ingests files into Bronze Delta tables.
- `resources/bronze_ingest_job.yml`: Databricks Job definition.
- `scripts/upload_data_to_volume.sh`: uploads local `data/` files to UC volume storage.

## Bronze target
- Catalog: `dev`
- Schema: `bronze`
- Source volume path: `/Volumes/dev/bronze/raw/data`
- Output tables: `dev.bronze.<folder_based_table_name>`

## Run
1. Validate and deploy bundle:
```bash
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
```

2. Upload local files from repo `data/`:
```bash
./scripts/upload_data_to_volume.sh <profile> dev bronze data
```

3. Run Bronze job:
```bash
databricks bundle run bronze_ingest -t dev --profile <profile>
```

## Notes
- The ingestion script auto-discovers leaf folders under the source root and creates one Bronze table per leaf folder.
- Defaults assume CSV (`header=true`, `infer_schema=true`). Override notebook parameters in the job if your files are JSON/Parquet.
