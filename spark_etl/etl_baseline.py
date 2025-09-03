import os, time
from pyspark.sql import SparkSession, functions as F

def timer():
    from time import perf_counter
    return perf_counter()

spark = (SparkSession.builder
         .appName("ETL Baseline")
         .master("local[8]")
         .config("spark.sql.shuffle.partitions", "48")
         .config("spark.driver.memory", "8g")
         .getOrCreate())

print("Spark UI: ", spark.sparkContext.uiWebUrl)

DATA="data"
OUT="results/baseline_customer_totals_csv"

t0 = timer()

customers = spark.read.option("header", "true").csv(os.path.join(DATA, "customers.csv"), inferSchema=True)
products = spark.read.option("header", "true").csv(os.path.join(DATA, "products.csv"), inferSchema=True)
orders = spark.read.option("header", "true").csv(os.path.join(DATA, "orders.csv"), inferSchema=True).withColumn("order_date", F.to_date("order_date"))

products = products.withColumnRenamed("unit_price", "product_unit_price")

fact = (orders.join(customers, "customer_id", "inner")
                .join(products, "product_id", "left")
                .withColumn("total_price", F.col("quantity") * F.col("unit_price")))

agg = (fact.groupBy("customer_id")
           .agg(F.sum("total_price").alias("total_spent"),
                F.countDistinct("order_id").alias("num_orders")))

t1 = timer()
agg.write.mode("overwrite").option("header", "true").csv(OUT)
t2 = timer()

print(f"[baseline] write: {t2 - t1:.2f}s")
print(f"[baseline] total: {t2 - t0:.2f}s")

spark.stop()
