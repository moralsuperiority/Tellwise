"""Spark Structured Streaming job: Kafka -> score with Redis -> store in PostgreSQL."""
import json
from datetime import datetime, timezone

import psycopg2
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from scoring import score_transaction

KAFKA_SERVERS = "localhost:9092"
TOPIC = "transactions"

schema = StructType([
    StructField("txn_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("device_id", StringType()),
    StructField("ts", StringType()),
    StructField("label", BooleanType()),
])

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
pg = psycopg2.connect(host="localhost", port=5432, dbname="fraud", user="fraud", password="fraud")

INSERT_SQL = """
INSERT INTO scored_transactions
(txn_id, user_id, amount, merchant, city, device_id, event_ts, processed_ts,
 latency_ms, risk_score, decision, reasons, true_label)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (txn_id) DO NOTHING
"""


def process_batch(batch_df, batch_id):
    rows = [row.asDict() for row in batch_df.orderBy("ts").collect()]  # oldest first
    if not rows:
        return
    out = []
    for tx in rows:
        event_time = datetime.fromisoformat(tx["ts"])
        tx["ts_epoch"] = event_time.timestamp()
        score, decision, reasons = score_transaction(r, tx)
        now = datetime.now(timezone.utc)
        latency_ms = int((now - event_time).total_seconds() * 1000)
        out.append((tx["txn_id"], tx["user_id"], tx["amount"], tx["merchant"], tx["city"],
                    tx["device_id"], event_time, now, latency_ms, score, decision,
                    json.dumps(reasons), tx["label"]))
    with pg.cursor() as cur:
        cur.executemany(INSERT_SQL, out)
    pg.commit()
    flagged = sum(1 for o in out if o[10] != "ALLOW")
    print(f"batch {batch_id}: {len(out)} transactions, {flagged} flagged")


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("fraud-engine").master("local[2]")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_SERVERS)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("t")).select("t.*")

    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", "checkpoints/fraud")
             .trigger(processingTime="2 seconds")
             .start())
    query.awaitTermination()
