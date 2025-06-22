import os
import pandas as pd
import psycopg2

db_params = {
    'host': 'localhost',  # Localhost since the DB is on your Mac
    'port': 5432,
    'dbname': 'oracle_db',
    'user': 'oracle',
    'password': 'oracle'
}
# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

def execute_query(query: str) -> list[dict]:  
    # Establish the connection
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    try:
        # Execute the query
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        conn.commit()
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return []
    finally:
        # Close the database connection
        cur.close()
        conn.close()


def load_csv_to_db(csv_file, table_name):
    # Read CSV into a pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Insert data into the table
    for index, row in df.iterrows():
        columns = ', '.join(df.columns)
        values = ', '.join([f"'{str(v)}'" for v in row.values])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cur.execute(insert_query)

    conn.commit()

# Upload products, sales, and inventory data
# load_csv_to_db('products.csv', 'products')
# load_csv_to_db('sales.csv', 'sales')
# load_csv_to_db('inventory.csv', 'inventory')

# Close the database connection
# cur.close()
# conn.close()

print("Data uploaded successfully!")
