import json
import os
from datetime import datetime
import yaml

from pyspark.sql import SparkSession, functions as F

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def read_checkpoint(path: str) -> str:
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        data = json.load(f)
        return data.get("last_max_value")

def write_checkpoint(path: str, last_max_value: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({"last_max_value": last_max_value,
                   "updated_at": datetime.utcnow().isoformat()}, f, indent=2)

def build_pushdown_query(cfg: dict, last_val: str) -> str:
    table = cfg["source"]["table"]
    cols = cfg["source"]["columns"]
    incr = cfg["source"]["incrementing_column"]
    cond = f"{incr} > '{last_val}'" if last_val else f"{incr} > '{cfg['source']['initial_lower_bound']}'"
    col_list = ", ".join(cols)
    return f"(SELECT {col_list} FROM {table} WHERE {cond}) AS t"

def apply_transforms(df, tcfg: dict):
    for old, new in (tcfg.get("renames") or {}).items():
        df = df.withColumnRenamed(old, new)
    for col, sql_lit in (tcfg.get("static_columns") or {}).items():
        df = df.withColumn(col, F.expr(sql_lit))
    for col, expr in (tcfg.get("formulas") or {}).items():
        df = df.withColumn(col, F.expr(expr))
    for col in (tcfg.get("drop_columns") or []):
        if col in df.columns:
            df = df.drop(col)
    df = df.withColumn("ingestion_ts", F.current_timestamp())
    return df

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    args = p.parse_args()

    cfg = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName(cfg["job"]["name"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", "jars/mysql-connector-j-8.4.0.jar")
        .config("spark.driver.extraClassPath", "jars/mysql-connector-j-8.4.0.jar")
        .config("spark.executor.extraClassPath", "jars/mysql-connector-j-8.4.0.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    cp_path = cfg["checkpoint"]["path"]
    last_val = read_checkpoint(cp_path)

    query = build_pushdown_query(cfg, last_val)
    jdbc_url = cfg["source"]["jdbc_url"]
    props = {
        "user": cfg["source"]["user"],
        "password": cfg["source"]["password"],
        "driver": cfg["source"]["driver"]
    }

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .options(**props)
        .load()
    )

    if df.rdd.isEmpty():
        print("No new rows. Exiting.")
        spark.stop()
        return

    df = apply_transforms(df, cfg["transform"])
    out_cols = cfg["select_output"]["columns"]
    df_out = df.select(*[F.col(c) for c in out_cols if c in df.columns])

    sink_kind = cfg["sink"]["kind"]
    path = cfg["sink"]["path"]
    os.makedirs(path, exist_ok=True)

    if sink_kind == "csv":
        opt = cfg["sink"].get("csv_options", {})
        df_out.coalesce(1).write.mode(opt.get("mode", "append")).options(**{k: str(v).lower() if isinstance(v, bool) else v for k,v in opt.items()}).csv(path)
    elif sink_kind == "parquet":
        df_out.write.mode("append").parquet(path)
    else:
        raise ValueError(f"Unsupported sink: {sink_kind}")

    incr = cfg["source"]["incrementing_column"]
    max_val = df.agg(F.max(F.col(incr)).cast("string")).collect()[0][0]
    write_checkpoint(cp_path, max_val)

    print(f"Wrote {df_out.count()} rows to {sink_kind} at {path}. Last {incr} = {max_val}")
    spark.stop()

if __name__ == "__main__":
    main()