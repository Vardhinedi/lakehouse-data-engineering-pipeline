import os
import sys
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import desc

# Windows PySpark Fix
python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Create Spark Session
spark = SparkSession.builder \
    .appName("Gold Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# =========================
# READ SILVER DATA
# =========================

orders_df = spark.read.csv(
    "data/silver/orders_clean.csv",
    header=True,
    inferSchema=True
)

customers_df = spark.read.csv(
    "data/silver/customers_clean.csv",
    header=True,
    inferSchema=True
)

# =========================
# JOIN DATASETS
# =========================

joined_df = orders_df.join(
    customers_df,
    on="customer_id",
    how="inner"
)

# =========================
# GOLD DATASET 1
# Revenue by Country
# =========================

revenue_by_country = joined_df.groupBy("country") \
    .agg(
        spark_sum("amount").alias("total_revenue")
    )

print("\nRevenue By Country")
revenue_by_country.show()

# =========================
# GOLD DATASET 2
# Top Customers
# =========================

top_customers = joined_df.groupBy("name") \
    .agg(
        spark_sum("amount").alias("total_spent")
    ) \
    .orderBy(desc("total_spent"))

print("\nTop Customers")
top_customers.show()

# =========================
# GOLD DATASET 3
# Daily Revenue
# =========================

daily_revenue = joined_df.groupBy("order_date") \
    .agg(
        spark_sum("amount").alias("daily_revenue")
    ) \
    .orderBy("order_date")

print("\nDaily Revenue")
daily_revenue.show()

# =========================
# SAVE GOLD DATASETS
# =========================

os.makedirs("data/gold", exist_ok=True)

revenue_by_country.toPandas().to_csv(
    "data/gold/revenue_by_country.csv",
    index=False
)

top_customers.toPandas().to_csv(
    "data/gold/top_customers.csv",
    index=False
)

daily_revenue.toPandas().to_csv(
    "data/gold/daily_revenue.csv",
    index=False
)

print("\nGold Layer Created Successfully!")

spark.stop()