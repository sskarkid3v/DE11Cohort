import os
from time import perf_counter
from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel
from schemas import CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA, ORDERS_SCHEMA    

def t(): return perf_counter()
def tf(ts): return f"{t()-ts:.2f}s"

DATA ="data"
BRONZE="bronze"
SILVER="silver"
OUT="results"

spark = (SparkSession.builder
            .appName("ETL Optimized")
            .master("local[28]")
            .config("spark.sql.shuffle.partitions", "96")
            .config("spark.driver.memory", "8g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))
            .config("spark.sql.files.maxPartitionBytes", str(64*1024*1024))
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(64*1024*1024))
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate())

print("Spark UI: ", spark.sparkContext.uiWebUrl)

to = t()
customers = spark.read.option("header", "true").schema(CUSTOMERS_SCHEMA).csv(os.path.join(DATA, "customers.csv"))
products = spark.read.option("header", "true").schema(PRODUCTS_SCHEMA).csv(os.path.join(DATA, "products.csv"))
orders = spark.read.option("header", "true").schema(ORDERS_SCHEMA).csv(os.path.join(DATA, "orders.csv")).withColumn("order_date", F.to_date("order_date"))

products = products.withColumnRenamed("unit_price", "product_unit_price")

print(f"[opt] read csv: {tf(to)}")

t1 = t()
(orders.write.mode("overwrite").option("compression","snappy").partitionBy("order_date").parquet(os.path.join(BRONZE, "orders")))
(customers.write.mode("overwrite").parquet(os.path.join(BRONZE, "customers")))
(products.write.mode("overwrite").parquet(os.path.join(BRONZE, "products")))
print(f"[opt] write bronze: {tf(t1)}")

t2 = t()
orders = spark.read.parquet(os.path.join(BRONZE, "orders"))
customers = spark.read.parquet(os.path.join(BRONZE, "customers"))
products = spark.read.parquet(os.path.join(BRONZE, "products"))
print(f"[opt] read bronze: {tf(t2)}")

orders = orders.repartition(48, "customer_id")

products_b = F.broadcast(products)

t3 = t()
fact = (orders.join(customers, "customer_id", "inner")
                .join(products_b, "product_id", "left")
                .withColumn("total_price", F.col("quantity") * F.col("unit_price"))
                .persist(StorageLevel.MEMORY_AND_DISK))
fact.count()
print(f"[opt] build fact: {tf(t3)}")

t4 = t()
(fact.select("order_id", "customer_id", "product_id", "quantity", "unit_price","order_date","total_price")
    .write.mode("overwrite").option("compression","snappy").partitionBy("order_date")
    .parquet(os.path.join(SILVER, "fact_orders")))
print(f"[opt] write silver: {tf(t4)}")

t5 = t()
agg_cust = (fact.groupBy("customer_id")
                    .agg(F.sum("total_price").alias("total_spent"),
                         F.countDistinct("order_id").alias("num_orders")))
agg_cust.write.mode("overwrite").option("header", "true").parquet(os.path.join(OUT, "customer_totals_paraquet"))
print(f"[opt] agg customer: {tf(t5)}")

t6 = t()
per_product_day = (fact.groupBy("product_id", "order_date")
                    .agg(F.sum("total_price").alias("revenue")))
(per_product_day.coalesce(8)
    .write.mode("overwrite").option("header", "true").option("compression","snappy")
    .parquet(os.path.join(OUT, "product_daily_revenue")))
print(f"[opt] agg product/day: {tf(t6)}")

print(f"[opt] total: {tf(to)}")
spark.stop()


# for further optimization ideas, see:
# 1. prune early, and broadcast both dims
# 2. add paralleism for orders when repatitioning
# 3. restructure the join plan for bronze layer
# 4. build a slim fact table with only the required columns for each aggregation
# 5. write fewer but bigger silver partitions


   

