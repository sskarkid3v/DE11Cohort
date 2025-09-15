from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

# 1) Spark session
spark = (SparkSession.builder
         .appName("kafka-to-postgres")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# 2) Kafka source (inside Compose network use kafka:9092)
kafka_bootstrap = "kafka:9092"
topic = "demo-topic"

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", kafka_bootstrap)
       .option("subscribe", topic)
       .option("startingOffsets", "earliest")
       .load())

# 3) Parse Kafka value (bytes) -> JSON columns
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product",  StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price",    DoubleType(), True),
    StructField("user_id",  IntegerType(), True),
    StructField("timestamp", LongType(),  True)
])

json_df = raw.selectExpr("CAST(value AS STRING) AS json_str") \
             .select(from_json(col("json_str"), schema).alias("data")) \
             .select("data.*")

# 4) Simple transform: compute total = quantity*price
enriched = json_df.withColumn("total", expr("quantity * price"))

# 5) Write to Postgres via foreachBatch
jdbc_url = "jdbc:postgresql://postgres:5432/retail"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
target_table = "public.orders_raw"

def write_batch(df, batch_id):
    (df.selectExpr(
         "timestamp as event_ts",
         "order_id", "product", "quantity", "price", "user_id"
     )
     .write
     .mode("append")
     .jdbc(url=jdbc_url, table=target_table, properties=jdbc_props))

query = (enriched
         .writeStream
         .outputMode("append")
         .foreachBatch(write_batch)
         .option("checkpointLocation", "/opt/app/checkpoints/orders_raw_ckpt")
         .start())

query.awaitTermination()
