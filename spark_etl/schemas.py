from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True)
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True)
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("order_date", StringType(), nullable=True)
])