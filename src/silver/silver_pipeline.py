import os
import sys
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# =========================
# ADD PROJECT ROOT TO PATH
# =========================

project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../..")
)

sys.path.append(project_root)

# Import quality checks
from src.utils.quality_checks import validate_orders

# =========================
# WINDOWS PYSPARK FIX
# =========================

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# =========================
# CREATE SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("Silver Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# =========================
# READ BRONZE DATA
# =========================

orders_df = spark.read.csv(
    "data/bronze/orders.csv",
    header=True,
    inferSchema=True
)

customers_df = spark.read.csv(
    "data/bronze/customers.csv",
    header=True,
    inferSchema=True
)

print("Original Orders Count:", orders_df.count())

# =========================
# DATA CLEANING
# =========================

# Remove negative amounts
orders_df = orders_df.filter(col("amount") > 0)

# Remove duplicate order IDs
orders_df = orders_df.dropDuplicates(["order_id"])

# Convert string date to proper date
orders_df = orders_df.withColumn(
    "order_date",
    to_date(col("order_time"))
)

print("Cleaned Orders Count:", orders_df.count())

# =========================
# DATA QUALITY CHECKS
# =========================

validate_orders(orders_df)

# =========================
# SHOW CLEAN DATA
# =========================

orders_df.show()

# =========================
# SAVE SILVER DATA
# =========================

os.makedirs("data/silver", exist_ok=True)

orders_df.toPandas().to_csv(
    "data/silver/orders_clean.csv",
    index=False
)

customers_df.toPandas().to_csv(
    "data/silver/customers_clean.csv",
    index=False
)

print("Silver Layer Created Successfully!")

spark.stop()