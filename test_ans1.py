import pandas as pd
import sqlite3
import json

df_a = pd.read_csv('test\\order_region_a.csv')
df_b = pd.read_csv('test\\order_region_b.csv')
df_a['region'] = 'A'
df_b['region'] = 'B'

# Combine dataframes
combined_df = pd.concat([df_a, df_b])

# Ensure PromotionDiscount is in a consistent format and convert to numeric
def extract_amount(discount_str):
    try:
        discount_dict = json.loads(discount_str.replace("'", '"'))  # Handle JSON-like strings
        return float(discount_dict.get('Amount', '0').replace(',', ''))
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing discount string: {discount_str} - {e}")
        return 0.0

combined_df['PromotionDiscount'] = combined_df['PromotionDiscount'].apply(extract_amount)

# Ensure all relevant columns are numeric
combined_df['QuantityOrdered'] = pd.to_numeric(combined_df['QuantityOrdered'], errors='coerce')
combined_df['ItemPrice'] = pd.to_numeric(combined_df['ItemPrice'], errors='coerce')

# Calculate total_sales and net_sale
combined_df['total_sales'] = combined_df['QuantityOrdered'] * combined_df['ItemPrice']
combined_df['net_sale'] = combined_df['total_sales'] - combined_df['PromotionDiscount']

# Exclude negative or zero net_sales
combined_df = combined_df[combined_df['net_sale'] > 0]

# Remove duplicate entries based on OrderId
combined_df = combined_df.drop_duplicates(subset='OrderId')

# Connect to SQLite database
conn = sqlite3.connect('sales_data.db')
cursor = conn.cursor()

# Create table
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    OrderId TEXT PRIMARY KEY,
    OrderItemId TEXT,
    QuantityOrdered INTEGER,
    ItemPrice INTEGER,
    PromotionDiscount REAL,
    batch_id INTEGER,
    total_sales REAL,
    region TEXT,
    net_sale REAL
)
''')

# Insert data into table
combined_df.to_sql('sales', conn, if_exists='replace', index=False)

# Commit changes
conn.commit()

# Define validation queries
queries = [
    "SELECT COUNT(*) FROM sales;",
    "SELECT OrderId, COUNT(*) FROM sales GROUP BY OrderId HAVING COUNT(*) > 1;",
    "SELECT * FROM sales WHERE net_sale <= 0;",
    "SELECT COUNT(*) FROM sales WHERE OrderId IS NULL OR OrderItemId IS NULL OR QuantityOrdered IS NULL OR ItemPrice IS NULL;"
]

# Execute and print results
try:
    for query in queries:
        print(f"Executing query: {query}")
        result = cursor.execute(query).fetchall()
        print("Result:", result)
finally:
    # Ensure connection is closed properly
    conn.close()