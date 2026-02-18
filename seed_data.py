import os
import random
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

import psycopg2
from faker import Faker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

# Set fixed seed for reproducibility
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

def random_date(start_date: datetime, end_date: datetime, peak_months: List[int] = None) -> datetime:
    """Generate a random datetime between start and end."""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    
    if peak_months and random.random() < 0.3: # 30% chance to force peak month if specified
         # Simplified peak logic: just bias standard generation
         pass

    # Simple uniform distribution for now, but we can bias towards Nov/Dec later in batch logic
    random_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_days)

def check_existing_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        count = cur.fetchone()[0]
        if count > 0:
            if "--force" not in sys.argv:
                print(f"WARNING: Database already contains {count} customers.")
                response = input("Do you want to continue and potentially duplicate data? (y/N): ")
                if response.lower() != 'y':
                    print("Aborting.")
                    sys.exit(0)
            else:
                print(f"Force flag detected. Proceeding despite existing {count} rows.")

def seed_customers(cur) -> List[int]:
    print("Inserting customers...", end=" ", flush=True)
    customers = []
    
    countries_dist = [
        ("USA", 35), ("UK", 15), ("Germany", 10), ("France", 10),
        ("Canada", 8), ("Australia", 7), ("Japan", 5),
        ("India", 5), ("Brazil", 3), ("Others", 2)
    ]
    
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2023, 12, 31)

    batch_data = []
    for _ in range(200):
        # Pick country
        r = random.randint(1, 100)
        cumulative = 0
        country = "Others"
        for c_name, c_pct in countries_dist:
            cumulative += c_pct
            if r <= cumulative:
                country = c_name
                break
        
        name = fake.name()
        # Ensure email uniqueness
        email = f"{name.replace(' ', '.').lower()}{random.randint(1000, 9999)}@example.com"
        created_at = random_date(start_date, end_date)
        
        batch_data.append((name, email, country, created_at))

    cur.executemany(
        "INSERT INTO customers (name, email, country, created_at) VALUES (%s, %s, %s, %s) RETURNING customer_id",
        batch_data
    )
    
    # We need IDs for foreign keys, but executemany with RETURNING is tricky in some drivers/versions.
    # Psycopg2 >= 2.9 supports it but we need to fetch results.
    # Actually, executemany returns None usually.
    # Better approach for IDs: insert one by one or fetch all after.
    # Let's fetch all IDs after insert.
    cur.execute("SELECT customer_id FROM customers ORDER BY customer_id DESC LIMIT 200")
    # This assumes no concurrent inserts, which is fine for seeding.
    # Wait, we prefer reliable IDs.
    # Let's just execute batch and then select all.
    row_ids = [row[0] for row in cur.fetchall()]
    customers = list(reversed(row_ids)) # Ordered by DESC, so reverse to match insertion order roughly (not guaranteed but fine)
    
    print(f"done ({len(customers)} rows)")
    return customers

def seed_products(cur) -> List[int]:
    print("Inserting products...", end=" ", flush=True)
    
    categories = {
        "Electronics": [
            ("Wireless Headphones", 89.99, 199.99), ("4K Smart TV", 299.99, 899.99), 
            ("Bluetooth Speaker", 29.99, 129.99), ("Mechanical Keyboard", 59.99, 159.99),
            ("Gaming Mouse", 29.99, 89.99), ("Webcam HD", 39.99, 99.99),
            ("USB-C Hub", 19.99, 59.99), ("Smart Watch", 99.99, 399.99),
            ("Noise Cancelling Earbuds", 49.99, 249.99), ("Portable Charger", 19.99, 49.99)
        ],
        "Clothing": [
            ("Running Shoes", 49.99, 149.99), ("Winter Jacket", 89.99, 199.99),
            ("Yoga Pants", 19.99, 59.99), ("Cotton T-Shirt", 9.99, 29.99),
            ("Denim Jeans", 39.99, 89.99), ("Casual Hoodie", 29.99, 69.99),
            ("Sport Socks", 9.99, 19.99), ("Leather Belt", 19.99, 49.99),
            ("Baseball Cap", 14.99, 29.99), ("Rain Coat", 49.99, 129.99)
        ],
        "Home & Kitchen": [
            ("Coffee Maker", 39.99, 149.99), ("Air Fryer", 59.99, 199.99),
            ("Robot Vacuum", 149.99, 449.99), ("Blender Pro", 49.99, 129.99),
            ("Instant Pot", 69.99, 129.99), ("Stand Mixer", 199.99, 399.99),
            ("Knife Set", 29.99, 149.99), ("Cutting Board", 14.99, 39.99),
            ("Non-stick Pan", 24.99, 69.99), ("Dish Rack", 19.99, 49.99)
        ],
        "Books": [
            ("Python Programming Guide", 29.99, 59.99), ("Data Science Handbook", 39.99, 59.99),
            ("The Art of SQL", 29.99, 49.99), ("Machine Learning Basics", 39.99, 59.99),
            ("Clean Code", 39.99, 49.99), ("Design Patterns", 39.99, 54.99),
            ("System Design Interview", 29.99, 39.99), ("Docker in Practice", 34.99, 49.99),
            ("Kubernetes Handbook", 39.99, 59.99), ("The Pragmatic Programmer", 29.99, 49.99)
        ],
        "Sports": [
            ("Yoga Mat", 19.99, 49.99), ("Resistance Bands", 9.99, 29.99),
            ("Dumbbells Set", 49.99, 199.99), ("Jump Rope", 9.99, 24.99),
            ("Foam Roller", 14.99, 39.99), ("Pull-up Bar", 29.99, 59.99),
            ("Gym Gloves", 14.99, 29.99), ("Water Bottle", 9.99, 34.99),
            ("Protein Shaker", 9.99, 19.99), ("Fitness Tracker", 49.99, 149.99)
        ]
    }

    batch_data = []
    
    # We want exact products but with random prices in range
    for category, products in categories.items():
        for name, min_p, max_p in products:
            price = round(random.uniform(min_p, max_p), 2)
            stock = random.randint(0, 500)
            batch_data.append((name, category, price, stock))

    cur.executemany(
        "INSERT INTO products (product_name, category, price, stock_quantity) VALUES (%s, %s, %s, %s)",
        batch_data
    )
    
    cur.execute("SELECT product_id, product_name, price FROM products")
    # Return list of dicts for easy lookup: {id, name, base_price}
    products = [{"id": row[0], "name": row[1], "price": float(row[2])} for row in cur.fetchall()]
    
    print(f"done ({len(products)} rows)")
    return products

def seed_orders(cur, customer_ids: List[int]) -> List[Dict[str, Any]]:
    print("Inserting orders...", end=" ", flush=True)
    
    # 1000 orders distributed
    years_config = {
        2022: 150,
        2023: 300,
        2024: 400,
        2025: 150 # Jan-Jun
    }
    
    orders = []
    
    # Zipf distribution for customer activity
    # Top 20% (40 customers) get 60% of orders (600 orders)
    # Remaining 160 customers get 40% (400 orders)
    # Simplify: shuffle customers, take first 40 as "active"
    random.shuffle(customer_ids)
    active_customers = customer_ids[:40]
    other_customers = customer_ids[40:]
    
    # We need to assign orders to years
    all_dates = []
    
    for year, count in years_config.items():
        for _ in range(count):
            month = random.randint(1, 12 if year != 2025 else 6)
            
            # 1.5x boost for Nov/Dec
            if year != 2025 and month in [11, 12]:
                # already picked month, but filtering distribution is handled by count allocation usually.
                # If we rely on random selection we need weighted month pick.
                # Let's purely randomise within the year but with weights
                weights = [1.0] * 12
                weights[10] = 1.5 # Nov
                weights[11] = 1.5 # Dec
                if year == 2025:
                    weights = [1.0] * 6 # Jan-Jun
                
                # Re-pick based on weights
                month = random.choices(range(1, len(weights)+1), weights=weights, k=1)[0]
            
            # Random day
            if month in [1, 3, 5, 7, 8, 10, 12]: d = random.randint(1, 31)
            elif month == 2: d = random.randint(1, 28) # simpler
            else: d = random.randint(1, 30)
            
            all_dates.append(datetime(year, month, d))

    random.shuffle(all_dates)
    
    batch_data = []
    
    for i, date in enumerate(all_dates):
        # Pick customer
        if i < 600: # First 60% of orders
            cid = random.choice(active_customers)
        else:
            cid = random.choice(other_customers)
            
        # Status
        r = random.random()
        if r < 0.55: status = 'delivered'
        elif r < 0.70: status = 'shipped'
        elif r < 0.82: status = 'processing'
        elif r < 0.90: status = 'pending'
        else: status = 'cancelled'
        
        # total_amount 0 initially
        orders.append({
            "order_date": date.date(),
            "customer_id": cid,
            "status": status,
            "total_amount": 0
        })
        batch_data.append((cid, date.date(), status, 0))

    cur.executemany(
        "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)",
        batch_data
    )
    
    # Retrieve order_ids (assuming serial)
    cur.execute("SELECT order_id, status, order_date FROM orders ORDER BY order_id")
    rows = cur.fetchall()
    
    # Re-construct orders list with real IDs
    final_orders = []
    for row in rows:
        final_orders.append({
            "order_id": row[0],
            "status": row[1],
            "order_date": row[2]
        })

    print(f"done ({len(final_orders)} rows)")
    return final_orders

def seed_order_items(cur, orders: List[Dict], products: List[Dict]):
    print("Inserting order_items...", end=" ", flush=True)
    
    batch_data = []
    
    # Weights for quantity
    qty_weights = [0.5, 0.25, 0.15, 0.10] # 1, 2, 3, 4+ (we'll limit to 5)
    qty_opts = [1, 2, 3, 4]
    
    # Top 10 products
    top_products = products[:10]
    other_products = products[10:]
    
    count = 0
    
    for order in orders:
        # 1-4 items
        num_items = random.choices([1, 2, 3, 4], weights=[0.3, 0.35, 0.25, 0.10], k=1)[0]
        
        # Select unique products
        # Weighting: top products 3x more likely
        # Just put them in a pool: 3x top, 1x others
        # To avoid recreating pool, sample logic:
        # We need `num_items` unique products.
        
        selected_products = []
        while len(selected_products) < num_items:
            if random.random() < 0.4: # 40% chance to pick from top
                p = random.choice(top_products)
            else:
                p = random.choice(other_products)
            
            if p not in selected_products:
                selected_products.append(p)
        
        for p in selected_products:
            # Quantity
            # If we picked 4 in weights above for range 1-5?
            # User said: 1=50%, 2=25%, 3=15%, 4+=10% (up to 5)
            # 4+ could be 4 or 5.
            r_q = random.random()
            if r_q < 0.5: q = 1
            elif r_q < 0.75: q = 2
            elif r_q < 0.90: q = 3
            elif r_q < 0.95: q = 4
            else: q = 5
            
            # Unit price variation
            base_price = p["price"]
            variation = random.uniform(0.95, 1.05)
            unit_price = round(base_price * variation, 2)
            
            batch_data.append((order["order_id"], p["id"], q, unit_price))
            count += 1
            
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

    print(f"done (~{count} rows)")

    # Update totals
    print("Updating order totals...", end=" ", flush=True)
    cur.execute("""
        UPDATE orders o
        SET total_amount = (
            SELECT COALESCE(SUM(quantity * unit_price), 0)
            FROM order_items
            WHERE order_id = o.order_id
        )
    """)
    print("done")

def seed_payments(cur, orders: List[Dict]):
    print("Inserting payments...", end=" ", flush=True)
    
    batch_data = []
    
    for order in orders:
        status = order["status"]
        should_pay = False
        
        r = random.random()
        if status == 'delivered': should_pay = True
        elif status == 'shipped': should_pay = (r < 0.90)
        elif status == 'processing': should_pay = (r < 0.70)
        elif status == 'pending': should_pay = (r < 0.20)
        else: should_pay = False # cancelled
        
        if should_pay:
            # We need the total amount to pay full
            # We can't use order['total_amount'] because it was 0 when loaded.
            # We must fetch updated amount or calculate?
            # Fetching 1000 orders again is cheap in seeding.
            pass
    
    # Let's fetch updated orders to get amounts
    cur.execute("SELECT order_id, total_amount FROM orders WHERE total_amount > 0")
    order_amounts = {row[0]: float(row[1]) for row in cur.fetchall()}
    
    count = 0
    for order in orders:
        oid = order["order_id"]
        status = order["status"]
        
        r = random.random()
        should_pay = False
        if status == 'delivered': should_pay = True
        elif status == 'shipped': should_pay = (r < 0.90)
        elif status == 'processing': should_pay = (r < 0.70)
        elif status == 'pending': should_pay = (r < 0.20)
        
        # Assuming order_amounts has the order (it should if it has items, which it should)
        if should_pay and oid in order_amounts:
            amount = order_amounts[oid]
            
            # Payment date
            odate = order["order_date"]
            # Convert odate to datetime if it's date
            # It's date from psycog2 likely. 
            # safe logic:
            s_date = datetime.combine(odate, datetime.min.time())
            p_date = random_date(s_date, s_date + timedelta(days=3))
            
            # Method
            rm = random.random()
            if rm < 0.4: method = 'credit_card'
            elif rm < 0.6: method = 'debit_card'
            elif rm < 0.8: method = 'paypal'
            elif rm < 0.95: method = 'bank_transfer'
            else: method = 'cash'
            
            batch_data.append((oid, p_date.date(), amount, method))
            count += 1

    cur.executemany(
        "INSERT INTO payments (order_id, payment_date, amount, method) VALUES (%s, %s, %s, %s)",
        batch_data
    )
    print(f"done ({count} rows)")

def main():
    conn = get_connection()
    check_existing_data(conn)
    
    start_time = time.time()
    
    try:
        with conn: # Transaction block
            with conn.cursor() as cur:
                customer_ids = seed_customers(cur)
                products = seed_products(cur)
                orders = seed_orders(cur, customer_ids)
                seed_order_items(cur, orders, products)
                seed_payments(cur, orders)
                
    except Exception as e:
        print(f"\nError during seeding: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

    elapsed = time.time() - start_time
    print(f"Seeding complete. Total time: {elapsed:.1f}s")

if __name__ == "__main__":
    main()
