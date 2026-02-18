import sys
import os
import psycopg2
from typing import List, Tuple, Any
from dotenv import load_dotenv

# Import config
sys.path.insert(0, "..")
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = "chatdb"
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

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

def print_result(check_name: str, passed: bool, details: str = ""):
    status = "[PASS]" if passed else "[FAIL]"
    print(f"{status:<8} {check_name}")
    if not passed and details:
        print(f"         Details: {details}")

def format_currency(val):
    return f"${val:,.2f}"

def run_query(cur, sql) -> List[Tuple]:
    cur.execute(sql)
    return cur.fetchall()

def main():
    conn = get_connection()
    
    print("\n" + "="*50)
    print("      DATABASE VERIFICATION & ANALYTICS")
    print("="*50 + "\n")

    try:
        with conn.cursor() as cur:
            # --- 1. DATA QUALITY CHECKS ---
            print("--- QUALITY CHECKS ---")
            
            # 1. NULL total_amounts
            cur.execute("SELECT COUNT(*) FROM orders WHERE total_amount IS NULL")
            count = cur.fetchone()[0]
            print_result("No NULL total_amounts", count == 0, f"{count} nulls found")
            
            # 2. Orphaned order_items (orders)
            cur.execute("SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL")
            count = cur.fetchone()[0]
            print_result("No orphaned order_items (order_id)", count == 0, f"{count} orphans")
            
            # 3. Orphaned order_items (products)
            cur.execute("SELECT COUNT(*) FROM order_items oi LEFT JOIN products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL")
            count = cur.fetchone()[0]
            print_result("No orphaned order_items (product_id)", count == 0, f"{count} orphans")
            
            # 4. Orphaned payments
            cur.execute("SELECT COUNT(*) FROM payments p LEFT JOIN orders o ON p.order_id = o.order_id WHERE o.order_id IS NULL")
            count = cur.fetchone()[0]
            print_result("No orphaned payments", count == 0, f"{count} orphans")
            
            # 5. Cancelled orders with payments
            cur.execute("SELECT COUNT(*) FROM payments p JOIN orders o ON p.order_id = o.order_id WHERE o.status = 'cancelled'")
            count = cur.fetchone()[0]
            print_result("No payments on cancelled orders", count == 0, f"{count} bad payments")
            
            # 6. Delivered orders missing payments
            cur.execute("SELECT COUNT(*) FROM orders o LEFT JOIN payments p ON o.order_id = p.order_id WHERE o.status = 'delivered' AND p.payment_id IS NULL")
            count = cur.fetchone()[0]
            print_result("All delivered orders have payment", count == 0, f"{count} missing")
            
            # 7. Duplicate emails
            cur.execute("SELECT COUNT(*) FROM (SELECT email FROM customers GROUP BY email HAVING COUNT(*) > 1) dupes")
            count = cur.fetchone()[0]
            print_result("No duplicate emails", count == 0, f"{count} dupes found")
            
            # 8. Order totals match items
            cur.execute("""
                SELECT COUNT(*) FROM orders o
                WHERE ABS(o.total_amount - COALESCE((
                    SELECT SUM(quantity * unit_price)
                    FROM order_items WHERE order_id = o.order_id
                ), 0)) > 0.05
            """)
            count = cur.fetchone()[0]
            print_result("Order totals match sum of items", count == 0, f"{count} mismatches")
            
            print("\n" + "-"*50 + "\n")
            
            # --- 2. ANALYTICS PREVIEW ---
            print("--- ANALYTICS PREVIEW ---")
            
            # A. Top 5 Customers
            print("\n[A] Top 5 Customers by Spending:")
            rows = run_query(cur, """
                SELECT c.name, SUM(o.total_amount) as spent
                FROM customers c JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.name ORDER BY spent DESC LIMIT 5
            """)
            for i, (name, spent) in enumerate(rows, 1):
                print(f"    {i}. {name:<25} {format_currency(spent)}")
                
            # B. Revenue by Category (2024)
            print("\n[B] Revenue by Category (2024):")
            rows = run_query(cur, """
                SELECT p.category, SUM(oi.quantity * oi.unit_price) as revenue
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE EXTRACT(YEAR FROM o.order_date) = 2024
                GROUP BY p.category ORDER BY revenue DESC
            """)
            for cat, rev in rows:
                print(f"    {cat:<20} {format_currency(rev)}")
                
            # C. Payment Methods
            print("\n[C] Payment Method Distribution:")
            rows = run_query(cur, """
                SELECT method, COUNT(*), 
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)
                FROM payments GROUP BY method ORDER BY count DESC
            """)
            for method, count, pct in rows:
                print(f"    {method:<15} {count:>4} ({pct}%)")
                
            # D. Top Products
            print("\n[D] Top 3 Products by Quantity Sold:")
            rows = run_query(cur, """
                SELECT p.product_name, SUM(oi.quantity) as total
                FROM order_items oi JOIN products p ON oi.product_id = p.product_id
                GROUP BY p.product_name ORDER BY total DESC LIMIT 3
            """)
            for i, (name, qty) in enumerate(rows, 1):
                print(f"    {i}. {name:<25} {qty} units")

    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
    finally:
        conn.close()

    print("\n" + "="*50)

if __name__ == "__main__":
    main()
