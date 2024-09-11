from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sqlite3
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .getOrCreate()

# Define SQLite database path
sqlite_db_path = "sales_data.db"



def extract_data(file_path):

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df



def transform_data(df):
    # Remove duplicates based on 'OrderId'
    df_cleaned = df.dropDuplicates(subset=['order_id'])

    # Filter rows where 'net_sales' > 0
    df_filtered = df_cleaned.filter(df_cleaned['net_sales'] > 0)

    # Calculate 'total_sales_amount' assuming 'price' and 'quantity' columns exist
    df_transformed = df_filtered.withColumn('total_sales_amount', F.col('price') * F.col('quantity'))

    return df_transformed



def load_data_to_sqlite(df, table_name, db_path):
    # Convert the DataFrame to pandas DataFrame
    pandas_df = df.toPandas()


    with sqlite3.connect(db_path) as conn:
        # Load the DataFrame into SQLite
        pandas_df.to_sql(table_name, conn, if_exists='replace', index=False)


# Step 4: Data Validation Functions
def count_total_records(df):
    return df.count()


def total_sales_by_region(df):
    return df.groupBy('region').agg(F.sum('total_sales_amount').alias('total_sales'))


def avg_sales_per_transaction(df):
    return df.agg(F.avg('total_sales_amount').alias('avg_sales_per_transaction'))


def check_for_duplicates(df):
    return df.groupBy('order_id').count().filter(F.col('count') > 1).count()


# Main function to run the ETL process
def run_etl(file_path, db_path, table_name):
    # 1. Extract Data
    sales_df = extract_data(file_path)

    # 2. Transform Data
    transformed_sales_df = transform_data(sales_df)

    # 3. Load Data into SQLite
    load_data_to_sqlite(transformed_sales_df, table_name, db_path)

    # 4. Validation
    # a) Count total records
    total_records = count_total_records(transformed_sales_df)
    print(f"Total number of records: {total_records}")

    # b) Total sales by region
    sales_by_region = total_sales_by_region(transformed_sales_df)
    print("Total sales by region:")
    sales_by_region.show()

    # c) Average sales amount per transaction
    avg_sales = avg_sales_per_transaction(transformed_sales_df)
    print("Average sales amount per transaction:")
    avg_sales.show()

    # d) Check for duplicate OrderId
    duplicate_count = check_for_duplicates(transformed_sales_df)
    if duplicate_count > 0:
        print(f"There are {duplicate_count} duplicate OrderId values.")
    else:
        print("No duplicate OrderId values found.")


# Example usage
if __name__ == "__main__":
    # Define file path and SQLite database path
    file_path = "test\\combined_sales.csv"  # Provide the correct file path
    sqlite_db_path = "sales_data.db"  # SQLite database file path
    table_name = "sales_data"

    # Run the ETL process
    run_etl(file_path, sqlite_db_path, table_name)
