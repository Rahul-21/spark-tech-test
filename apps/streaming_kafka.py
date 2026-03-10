import yaml
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T


def load_cfg(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_schema(cfg: Dict) -> T.StructType:
    mapping = {
        "string": T.StringType(),
        "long": T.LongType(),
        "double": T.DoubleType(),
        "timestamp": T.TimestampType(),
        "boolean": T.BooleanType(),
        "int": T.IntegerType(),
    }

    fields = [
        T.StructField(f["name"], mapping[f["type"]], True)
        for f in cfg["schema"]["fields"]
    ]

    fields.append(T.StructField("_corrupt_record", T.StringType(), True))

    return T.StructType(fields)


def apply_transforms(df: DataFrame, tcfg: Dict) -> DataFrame:
    for col, sql_lit in (tcfg.get("static_columns") or {}).items():
        df = df.withColumn(col, F.expr(sql_lit))

    for col, expr in (tcfg.get("formulas") or {}).items():
        df = df.withColumn(col, F.expr(expr))

    if tcfg.get("watermark") and tcfg.get("event_time_col"):
        df = df.withWatermark(tcfg["event_time_col"], tcfg["watermark"])

    df = df.withColumn("ingestion_ts", F.current_timestamp())

    return df


def foreach_batch_writer(kafka_bootstrap: str, metrics_topic: str):
    def fn(batch_df: DataFrame, batch_id: int):
        cnt = batch_df.count()

        spark = batch_df.sparkSession
        metrics_df = spark.createDataFrame(
            [(int(batch_id), int(cnt), datetime.utcnow().isoformat())],
            schema="batch_id INT, count INT, ts STRING",
        )

        out = metrics_df.select(F.to_json(F.struct("*")).alias("value"))

        (
            out.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("topic", metrics_topic)
            .save()
        )

    return fn


def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    args = p.parse_args()

    cfg = load_cfg(args.config)
    kcfg = cfg["kafka"]
    sink_cfg = cfg["sink"]
    dlq_cfg = cfg.get("dlq") or {}
    tcfg = cfg.get("transform") or {}
    streaming_cfg = cfg.get("streaming") or {}

    spark = (
        SparkSession.builder
        .appName(cfg["job"]["name"])
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = build_schema(cfg)

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kcfg["bootstrap_servers"])
        .option("subscribe", kcfg["subscribe"])
        .option("startingOffsets", kcfg.get("startingOffsets", "latest"))
        .option("failOnDataLoss", str(kcfg.get("failOnDataLoss", False)).lower())
    )

    if kcfg.get("maxOffsetsPerTrigger"):
        reader = reader.option(
            "maxOffsetsPerTrigger", int(kcfg["maxOffsetsPerTrigger"])
        )

    raw = reader.load()

    rawkv = raw.select(
        F.col("key").cast("string").alias("key_str"),
        F.col("value").cast("string").alias("value_str"),
        "topic",
        "partition",
        "offset",
        "timestamp",
    )

    decoded = rawkv.select(
        "*",
        F.from_json(
            F.col("value_str"),
            schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ).alias("data"),
    )

    good = decoded.where(F.col("data._corrupt_record").isNull()).select("data.*")
    bad = decoded.where(F.col("data._corrupt_record").isNotNull()).select(
        F.col("value_str").alias("raw_value"),
        F.col("data._corrupt_record").alias("corrupt_reason"),
        "topic",
        "partition",
        "offset",
        "timestamp",
    )


    transformed = apply_transforms(good, tcfg)

    trigger = streaming_cfg.get("trigger", "10 seconds")


    main_query = (
        transformed.writeStream.outputMode("append")
        .format("parquet")
        .option("path", sink_cfg["path"])
        .option("checkpointLocation", sink_cfg["checkpoint"])
        .trigger(processingTime=trigger)
        .start()
    )


    if dlq_cfg.get("enabled", True):
        dlq_checkpoint = dlq_cfg.get(
            "checkpoint", sink_cfg["checkpoint"] + "/_dlq"
        )

        if dlq_cfg.get("kind", "kafka") == "kafka":
            dlq_topic = dlq_cfg.get("topic", "transactions_dlq")
            dlq_out = bad.select(F.to_json(F.struct("*")).alias("value"))

            dlq_query = (
                dlq_out.writeStream.format("kafka")
                .option("kafka.bootstrap.servers", kcfg["bootstrap_servers"])
                .option("topic", dlq_topic)
                .option("checkpointLocation", dlq_checkpoint)
                .trigger(processingTime=trigger)
                .start()
            )

        else:
            dlq_path = dlq_cfg.get("path", "data/out/stream/dlq")

            dlq_query = (
                bad.writeStream.format("parquet")
                .option("path", dlq_path)
                .option("checkpointLocation", dlq_checkpoint)
                .trigger(processingTime=trigger)
                .start()
            )


    metrics_checkpoint = sink_cfg["checkpoint"] + "/_metrics"

    metrics_query = (
        transformed.writeStream.outputMode("append")
        .option("checkpointLocation", metrics_checkpoint)
        .trigger(processingTime=trigger)
        .foreachBatch(
            foreach_batch_writer(
                kcfg["bootstrap_servers"], kcfg["metrics_topic"]
            )
        )
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()