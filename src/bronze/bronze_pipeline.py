import os
import sys
import pandas as pd

from pyspark.sql import SparkSession

# Windows PySpark Fix
python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Create Spark Session
spark = SparkSession.builder \
    .appName("Bronze Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# Read raw JSON files
orders_df = spark.read.json("data/raw/orders.json")
customers_df = spark.read.json("data/raw/customers.json")

# Print schemas
print("Orders Schema:")
orders_df.printSchema()

print("Customers Schema:")
customers_df.printSchema()

# Convert Spark DF -> Pandas DF
orders_pd = orders_df.toPandas()
customers_pd = customers_df.toPandas()

# Create output folders
os.makedirs("data/bronze", exist_ok=True)

# Save as CSV
orders_pd.to_csv("data/bronze/orders.csv", index=False)
customers_pd.to_csv("data/bronze/customers.csv", index=False)

print("Bronze Layer Created Successfully!")

spark.stop()