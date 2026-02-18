import sys
import os
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import psycopg2
from faker import Faker
from dotenv import load_dotenv

# Import config from parent directory
sys.path.insert(0, "..")

# Load environment variables
# Load environment variables
from pathlib import Path
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "chatdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

# Set fixed seed
random.seed(42)
Faker.seed(42)
fake = Faker(['en_US', 'en_GB', 'de_DE', 'fr_FR', 'ja_JP'])

def get_connection():
    try:
        return psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def check_existing_data(conn):
    with conn.cursor() as cur:
        # Check if tables exist first to avoid error if script run before DDL
        cur.execute("SELECT to_regclass('public.customers')")
        if not cur.fetchone()[0]:
            print("Error: Tables do not exist. Please run create_tables.sql first.")
            sys.exit(1)
            
        cur.execute("SELECT COUNT(*) FROM customers")
        count = cur.fetchone()[0]
        if count > 0:
            if "--force" not in sys.argv:
                print(f"WARNING: Database already contains {count} customers.")
                print("Use --force to append/overwrite (though this script appends).")
                print("Aborting to prevent duplication.")
                sys.exit(0)
            else:
                print(f"Force flag detected. Proceeding...")

def seed_customers(cur) -> List[int]:
    print("✓ customers   : Inserting...", end="\r")
    
    countries_dist = [
        ("USA", 35), ("UK", 15), ("Germany", 10), ("France", 10),
        ("Canada", 8), ("Australia", 7), ("Japan", 5),
        ("India", 5), ("Brazil", 3), ("Others", 2)
    ]
    
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    batch_data = []
    ids = []
    
    # Pre-generate IDs to avoid fetch overhead if possible, but SERIAL relies on DB.
    # We'll insert and then fetchall IDs.
    
    for _ in range(2500):
        # Country
        r = random.randint(1, 100)
        cumulative = 0
        country = "Others"
        for c_name, c_pct in countries_dist:
            cumulative += c_pct
            if r <= cumulative:
                country = c_name
                break
        
        name = fake.name()
        email = f"{name.replace(' ', '.').lower()}{random.randint(1000, 9999)}@example.com"
        created_at = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        
        batch_data.append((name, email, country, created_at))

    # Batch insert
    cur.executemany(
        "INSERT INTO customers (name, email, country, created_at) VALUES (%s, %s, %s, %s)",
        batch_data
    )
    
    # Fetch IDs
    cur.execute("SELECT customer_id FROM customers ORDER BY customer_id DESC")
    rows = cur.fetchall()
    ids = [r[0] for r in rows] # Reverse order usually
    ids.reverse() # Roughly matching insert order
    
    print(f"✓ customers   : {len(ids)} rows inserted")
    return ids

def seed_products(cur) -> List[Dict]:
    print("✓ products    : Inserting...", end="\r")
    
    categories = {
        "Electronics": [
            ("Wireless Headphones", 89.99), ("4K Smart TV", 499.99), ("Bluetooth Speaker", 49.99),
            ("Mechanical Keyboard", 129.99), ("Gaming Mouse", 59.99), ("Webcam HD", 79.99),
            ("USB-C Hub", 39.99), ("Smart Watch", 199.99), ("Noise Cancelling Earbuds", 149.99), ("Portable Charger", 29.99)
        ],
        "Clothing": [
            ("Running Shoes", 89.99), ("Winter Jacket", 149.99), ("Yoga Pants", 49.99),
            ("Cotton T-Shirt", 19.99), ("Denim Jeans", 69.99), ("Casual Hoodie", 49.99),
            ("Sport Socks", 14.99), ("Leather Belt", 39.99), ("Baseball Cap", 24.99), ("Rain Coat", 79.99)
        ],
        "Home & Kitchen": [
            ("Coffee Maker", 89.99), ("Air Fryer", 119.99), ("Robot Vacuum", 299.99),
            ("Blender Pro", 89.99), ("Instant Pot", 99.99), ("Stand Mixer", 249.99),
            ("Knife Set", 69.99), ("Cutting Board", 29.99), ("Non-stick Pan", 49.99), ("Dish Rack", 39.99)
        ],
        "Books": [
            ("Python Programming", 49.99), ("Data Science Handbook", 59.99), ("SQL Mastery", 39.99),
            ("Machine Learning", 69.99), ("Clean Code", 44.99), ("Design Patterns", 54.99),
            ("System Design", 49.99), ("Docker Guide", 39.99), ("Kubernetes Book", 59.99), ("Pragmatic Programmer", 49.99)
        ],
        "Sports": [
            ("Yoga Mat", 29.99), ("Resistance Bands", 19.99), ("Dumbbells Set", 89.99),
            ("Jump Rope", 14.99), ("Foam Roller", 24.99), ("Pull-up Bar", 39.99),
            ("Gym Gloves", 19.99), ("Water Bottle", 14.99), ("Protein Shaker", 9.99), ("Fitness Tracker", 99.99)
        ]
    }

    batch_data = []
    for cat, items in categories.items():
        for name, price in items:
            # Create variations to expand catalog
            base_stock = random.randint(0, 500)
            
            # Original
            batch_data.append((name, cat, price, base_stock))
            
            # Variations (30% chance for Pro, 30% for Lite)
            if random.random() < 0.3:
                batch_data.append((f"{name} Pro", cat, round(price * 1.4, 2), random.randint(0, 200)))
            if random.random() < 0.3:
                batch_data.append((f"{name} Lite", cat, round(price * 0.7, 2), random.randint(0, 800)))

    cur.executemany(
        "INSERT INTO products (product_name, category, price, stock_quantity) VALUES (%s, %s, %s, %s)",
        batch_data
    )
    
    cur.execute("SELECT product_id, product_name, price FROM products")
    products = [{"id": r[0], "name": r[1], "price": float(r[2])} for r in cur.fetchall()]
    
    print(f"✓ products    : {len(products)} rows inserted")
    return products

def seed_orders(cur, customer_ids: List[int]) -> List[Dict]:
    print("✓ orders      : Inserting...", end="\r")
    
    # Power law distribution for customers
    # Shuffle IDs
    pool = customer_ids[:]
    random.shuffle(pool)
    top_20_count = int(len(pool) * 0.2)
    top_customers = pool[:top_20_count]
    bottom_customers = pool[top_20_count:]
    
    years = {2022: 2000, 2023: 3500, 2024: 4500, 2025: 2000}
    orders = []
    
    for year, count in years.items():
        for _ in range(count):
            # Customer
            if random.random() < 0.6: # 60% orders from top 20%
                cid = random.choice(top_customers)
            else:
                cid = random.choice(bottom_customers)
            
            # Month (weighted for Nov/Dec)
            weights = [1.0] * 12
            weights[10] = 1.5 # Nov (index 10)
            weights[11] = 1.5 # Dec (index 11)
            if year == 2025:
                weights = [1.0] * 6 # Only Jan-Jun
                month = random.choices(range(1, 7), weights=weights, k=1)[0]
            else:
                month = random.choices(range(1, 13), weights=weights, k=1)[0]
                
            day = random.randint(1, 28) # Safe day
            date = datetime(year, month, day).date()
            
            # Status
            r = random.random()
            if r < 0.55: status = 'delivered'
            elif r < 0.70: status = 'shipped'
            elif r < 0.82: status = 'processing'
            elif r < 0.90: status = 'pending'
            else: status = 'cancelled'
            
            orders.append({
                "customer_id": cid,
                "order_date": date,
                "status": status,
                "total_amount": 0.0 # Calc later
            })

    # Batch insert
    batch_rows = [(o["customer_id"], o["order_date"], o["status"], 0) for o in orders]
    cur.executemany(
        "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)",
        batch_rows
    )
    
    # Re-fetch for IDs
    cur.execute("SELECT order_id, customer_id, order_date, status FROM orders")
    rows = cur.fetchall()
    
    final_orders = []
    for r in rows:
        final_orders.append({
            "order_id": r[0],
            "customer_id": r[1],
            "order_date": r[2],
            "status": r[3]
        })
        
    print(f"✓ orders      : {len(final_orders)} rows inserted")
    return final_orders

def seed_items_and_update_totals(cur, orders: List[Dict], products: List[Dict]):
    print("✓ order_items : Inserting...", end="\r")
    
    batch_data = []
    
    order_totals = {} # oid -> total
    
    for order in orders:
        oid = order["order_id"]
        # 1-4 items
        num_items = random.randint(1, 4)
        
        # Pick products (no duplicates in order)
        selected = random.sample(products, num_items)
        
        total = 0.0
        
        for p in selected:
            qty = random.randint(1, 5) # Reasonable qty
            unit_price = round(p["price"] * random.uniform(0.95, 1.05), 2)
            
            batch_data.append((oid, p["id"], qty, unit_price))
            total += qty * unit_price
            
        order_totals[oid] = total
        
        if len(batch_data) >= 100:
            cur.executemany(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                batch_data
            )
            batch_data = []
            
    if batch_data:
        cur.executemany(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            batch_data
        )
        
    print(f"✓ order_items : {cur.rowcount} rows inserted (approx)") # rowcount might be last batch
    
    # Update totals
    print("✓ orders      : Updating totals...", end="\r")
    
    # Batch update efficiently? 
    # Option A: CASE statement. Option B: executemany UPDATE.
    # Option C: SQL pure update (as requested in prompt "UPDATE orders SET total_amount = SUM...")
    
    cur.execute("""
        UPDATE orders o
        SET total_amount = (
            SELECT COALESCE(SUM(quantity * unit_price), 0)
            FROM order_items
            WHERE order_id = o.order_id
        )
    """)
    print("✓ orders      : Totals updated")
    
    # Pass back updated orders (with totals) for payments
    for o in orders:
        if o["order_id"] in order_totals:
            o["total_amount"] = order_totals[o["order_id"]]
            
    return orders

def seed_payments(cur, orders: List[Dict]):
    print("✓ payments    : Inserting...", end="\r")
    
    batch_data = []
    count = 0
    
    for order in orders:
        status = order["status"]
        if status == 'cancelled': continue
        
        # Payment probability
        prob = 0.0
        if status == 'delivered': prob = 1.0
        elif status == 'shipped': prob = 0.9
        elif status == 'processing': prob = 0.7
        elif status == 'pending': prob = 0.2
        
        if random.random() < prob:
            oid = order["order_id"]
            amount = order["total_amount"]
            
            # Date: order_date + 0-3 days
            pdate = order["order_date"] + timedelta(days=random.randint(0, 3))
            
            # Method
            r = random.random()
            if r < 0.4: method = 'credit_card'
            elif r < 0.6: method = 'debit_card'
            elif r < 0.8: method = 'paypal'
            elif r < 0.95: method = 'bank_transfer'
            else: method = 'cash'
            
            batch_data.append((oid, pdate, amount, method))
            count += 1
            
    if batch_data:
        cur.executemany(
            "INSERT INTO payments (order_id, payment_date, amount, method) VALUES (%s, %s, %s, %s)",
            batch_data
        )
        
    print(f"✓ payments    : {count} rows inserted")

def main():
    start_time = time.time()
    conn = get_connection()
    check_existing_data(conn)
    
    print("Starting data seed...")
    try:
        with conn:
            with conn.cursor() as cur:
                c_ids = seed_customers(cur)
                prods = seed_products(cur)
                orders = seed_orders(cur, c_ids)
                orders = seed_items_and_update_totals(cur, orders, prods)
                seed_payments(cur, orders)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

    elapsed = time.time() - start_time
    print(f"\nSeeding complete in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()
