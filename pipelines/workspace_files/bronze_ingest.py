# Databricks notebook source
import re
from typing import List

from pyspark.sql import DataFrame

# COMMAND ----------

def _get_param(name: str, default_value: str) -> str:
    dbutils.widgets.text(name, default_value)
    return dbutils.widgets.get(name).strip() or default_value


def _sanitize_identifier(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown"


def _discover_leaf_dirs(path: str) -> List[str]:
    # Returns leaf directories that contain at least one file.
    stack = [path.rstrip("/")]
    leaf_dirs: List[str] = []

    while stack:
      current = stack.pop()
      entries = dbutils.fs.ls(current)
      dirs = [e.path.rstrip("/") for e in entries if e.isDir()]
      files = [e.path for e in entries if not e.isDir()]
      if files:
          leaf_dirs.append(current)
      stack.extend(dirs)

    return sorted(set(leaf_dirs))


def _build_reader(source_format: str, infer_schema: str, header: str):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.inferColumnTypes", infer_schema)
        .option("header", header)
    )

    if source_format.lower() == "csv":
        reader = reader.option("multiLine", "false")

    return reader


def _await_all(queries) -> None:
    for query in queries:
        query.awaitTermination()


catalog = _get_param("catalog", "dev")
schema = _get_param("schema", "bronze")
source_root = _get_param("source_root", f"/Volumes/{catalog}/{schema}/raw/data")
source_format = _get_param("source_format", "csv")
header = _get_param("header", "true")
infer_schema = _get_param("infer_schema", "true")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

leaf_dirs = _discover_leaf_dirs(source_root)
if not leaf_dirs:
    raise ValueError(f"No input files found under {source_root}")

queries = []
for input_path in leaf_dirs:
    relative = input_path.replace(source_root.rstrip("/") + "/", "")
    table_name = _sanitize_identifier(relative.replace("/", "_"))
    full_table_name = f"{catalog}.{schema}.{table_name}"
    checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/{table_name}"

    df: DataFrame = _build_reader(source_format, infer_schema, header).load(input_path)

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .outputMode("append")
        .toTable(full_table_name)
    )
    queries.append(query)

_await_all(queries)

print(f"Bronze load complete. Created/updated {len(leaf_dirs)} table(s) in {catalog}.{schema}.")
