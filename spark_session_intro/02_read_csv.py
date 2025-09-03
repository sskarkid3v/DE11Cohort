from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntroToSpark").getOrCreate()

df = spark.read.csv("sales.csv", header=True, inferSchema=True)

#df.select("customer", "product").show()

#df.filter(df["price"] > 500).show()

#result = df.withColumn("total", df["quantity"] * df["price"]). groupBy("customer").sum("total")

#selct customer, sum(quanity * price) as total from sales group by customer
#result.show()

#df.createOrReplaceTempView("sales")
#spark.sql("Select customer, sum(quantity * price) as total from sales group by customer").show()

#df.write.mode("overwrite").parquet("sales_parquet")
#paraquet_df = spark.read.parquet("sales_parquet")
#paraquet_df.show()


#total revenue by product
filtered=df.withColumn("total", df["quantity"] * df["price"]).groupBy("product").sum("total")
filtered.show()

df.createOrReplaceTempView("sales")

#top 5 customers by spending
spark.sql("""
          select customer, sum(quantity * price) as revenue
          from sales
          group by customer
          order by revenue desc
          limit 5
          """).show()

#find daily order counts

spark.sql("""
          select order_date, count(*) as order_count
          from sales
          group by order_date
          order by order_date
          """).show()