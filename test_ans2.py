from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract
from pyspark.sql.types import DoubleType
import pandas as pd
import sqlite3

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Sales Data ETL") \
    .getOrCreate()

# Define file paths (update with actual paths if needed)
path_a = "test\\order_region_a.csv"
path_b = "test\\order_region_b.csv"

# Read data
df_a = spark.read.csv(path_a, header=True, inferSchema=True)
df_b = spark.read.csv(path_b, header=True, inferSchema=True)

# Add region column
df_a = df_a.withColumn("region", lit("A"))
df_b = df_b.withColumn("region", lit("B"))

# Combine dataframes
combined_df = df_a.union(df_b)

# Use regex to extract the amount from the PromotionDiscount field
regex_pattern = r'\{ "CurrencyCode": "INR", "Amount": "([^"]+)"\}'

# Extract the amount as a string
combined_df = combined_df.withColumn("PromotionDiscount", 
                                     regexp_extract(col("PromotionDiscount"), regex_pattern, 1))

# Ensure numeric columns
combined_df = combined_df.withColumn("QuantityOrdered", col("QuantityOrdered").cast("integer"))
combined_df = combined_df.withColumn("ItemPrice", col("ItemPrice").cast("integer"))
combined_df = combined_df.withColumn("PromotionDiscount", col("PromotionDiscount").cast(DoubleType()))

# Calculate total_sales and net_sale
combined_df = combined_df.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))
combined_df = combined_df.withColumn("net_sale", col("total_sales") - col("PromotionDiscount"))

# Filter out negative or zero net_sales
combined_df = combined_df.filter(col("net_sale") > 0)

# Remove duplicates based on OrderId
combined_df = combined_df.dropDuplicates(["OrderId"])

# Convert to Pandas DataFrame
pdf = combined_df.toPandas()

# Define SQLite database path
sqlite_db_path = "sales_data.db"

# Load DataFrame into SQLite database
conn = sqlite3.connect(sqlite_db_path)
pdf.to_sql('sales_data', conn, if_exists='replace', index=False)

# Define SQL queries for validation
queries = {
    "data":"SELECT * FROM sales_data;",
    "count_records": "SELECT COUNT(*) FROM sales_data;",
    "total_sales_by_region": "SELECT region, SUM(total_sales) FROM sales_data GROUP BY region;",
    "average_sales_per_transaction": "SELECT AVG(total_sales) FROM sales_data;",
    "check_duplicates": "SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1;"
}

# Function to execute and print query results
def execute_queries(conn, queries):
    cursor = conn.cursor()
    for key, query in queries.items():
        print(f"Executing query for {key}: {query}")
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Results for {key}: {results}")

# Execute and print results
execute_queries(conn, queries)

# Close the SQLite connection
conn.close()

# Stop SparkSession
spark.stop()
