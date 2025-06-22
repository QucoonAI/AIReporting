import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

NUM_PRODUCTS = 50
NUM_SALES = 1000
DAYS_BACK = 90 

def generate_products(n_products):
    categories = ["Beverages", "Bakery", "Produce", "Dairy", "Meat", "Frozen", "Snacks"]
    products = []
    for pid in range(1, n_products + 1):
        products.append({
            "product_id": pid,
            "name": fake.word().capitalize(),
            "category": random.choice(categories),
            "price": round(random.uniform(1.0, 100.0), 2)
        })
    return pd.DataFrame(products)

def generate_sales(products_df, num_sales, days_back):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    sales = []
    for i in range(num_sales):
        prod = products_df.sample(1).iloc[0]
        sale_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        quantity = random.randint(1, 10)
        sales.append({
            "sale_id": i + 1,
            "product_id": prod.product_id,
            "date": sale_date.strftime('%Y-%m-%d %H:%M:%S'),
            "quantity": quantity,
            "total": round(quantity * prod.price, 2)
        })
    return pd.DataFrame(sales)

def generate_inventory(products_df, days_back):
    records = []
    end_date = datetime.now().date()
    for day in range(days_back):
        current = end_date - timedelta(days=day)
        for _, prod in products_df.iterrows():
            records.append({
                "product_id": prod.product_id,
                "date": current.strftime('%Y-%m-%d'),
                "stock_level": random.randint(0, 200)
            })
    return pd.DataFrame(records)

if __name__ == '__main__':
    products_df = generate_products(NUM_PRODUCTS)
    sales_df = generate_sales(products_df, NUM_SALES, DAYS_BACK)
    inventory_df = generate_inventory(products_df, DAYS_BACK)
    products_df.to_csv('products.csv', index=False)
    sales_df.to_csv('sales.csv', index=False)
    inventory_df.to_csv('inventory.csv', index=False)
    print("Synthetic data generated: products.csv, sales.csv, inventory.csv")
