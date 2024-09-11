import pandas as pd
import sqlite3
sales_a = pd.read_csv('test\\sales_region_a.csv')
sales_b = pd.read_csv('test\\sales_region_b.csv')
sales_a['region'] = 'A'
sales_b['region'] = 'B'

combined_sales = pd.concat([sales_a, sales_b])
combined_sales['total_sales'] = combined_sales['quantity'] *(combined_sales['price'] )
combined_sales['net_sales'] = combined_sales['quantity'] *(combined_sales['price'] - combined_sales['discount_price'])
combined_sales.rename(columns={'id': 'order_id'}, inplace=True)
print(combined_sales)
combined_sales.to_csv('test\\combined_sales.csv', index=False)


combined_sales['date'] = pd.to_datetime(combined_sales['date']).dt.strftime('%Y-%m-%d')


combined_sales.drop_duplicates(subset=['order_id'], inplace=True)


conn = sqlite3.connect('sales_data.db')
cursor = conn.cursor()

# Create a table in the SQLite database
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        OrderId INTEGER PRIMARY KEY,
        date TEXT,
        OrderItemId INTEGER,
        quantity INTEGER,
        price REAL,
        total_sales REAL,
        region TEXT,
        PromotionDiscount INTEGER
    )
''')

# Insert the transformed data into the database
combined_sales.to_sql('sales', conn, if_exists='replace', index=False)

# Commit and close the connection
conn.commit()
conn.close()


# Step 4: Validate data with SQL queries
def validate_data():
    conn = sqlite3.connect('sales_data.db')
    cursor = conn.cursor()

    # Query 1: Count total records
    cursor.execute("SELECT COUNT(*) FROM sales")
    total_records = cursor.fetchone()[0]
    print(f"Total records: {total_records}")

    # Query 2: Ensure no duplicate ids
    cursor.execute("SELECT COUNT(DISTINCT order_id) FROM sales")
    unique_ids = cursor.fetchone()[0]
    print(f"Unique ids: {unique_ids}")

    # Query 3: Check if any date is not in the correct format
    cursor.execute("SELECT DISTINCT date FROM sales WHERE date NOT LIKE '____-__-__'")
    invalid_dates = cursor.fetchall()
    if invalid_dates:
        print("Invalid dates found:")
        for row in invalid_dates:
            print(row[0])
    else:
        print("All dates are in correct format. ")

    conn.close()


# Run the validation function
validate_data()
