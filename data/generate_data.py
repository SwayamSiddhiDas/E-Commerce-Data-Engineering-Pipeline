"""
Data Generator for E-Commerce Pipeline
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

PRODUCTS = [
    ("P001", "Laptop", "Electronics", 45000),
    ("P002", "Smartphone", "Electronics", 25000),
    ("P003", "Headphones", "Electronics", 3000),
    ("P004", "Desk Chair", "Furniture", 8000),
    ("P005", "Notebook", "Stationery", 150),
    ("P006", "Pen Set", "Stationery", 200),
    ("P007", "Monitor", "Electronics", 18000),
    ("P008", "Keyboard", "Electronics", 2500),
    ("P009", "Bookshelf", "Furniture", 5000),
    ("P010", "Backpack", "Accessories", 1500),
]

CUSTOMERS = [
    ("C001", "Aarav Sharma", "Mumbai", "Maharashtra"),
    ("C002", "Priya Patel", "Ahmedabad", "Gujarat"),
    ("C003", "Rohan Verma", "Delhi", "Delhi"),
    ("C004", "Sneha Iyer", "Chennai", "Tamil Nadu"),
    ("C005", "Karan Mehta", "Pune", "Maharashtra"),
    ("C006", "Ananya Roy", "Kolkata", "West Bengal"),
    ("C007", "Vikram Singh", "Jaipur", "Rajasthan"),
    ("C008", "Divya Nair", "Bangalore", "Karnataka"),
    ("C009", "Arjun Gupta", "Hyderabad", "Telangana"),
    ("C010", "Meera Joshi", "Bhopal", "Madhya Pradesh"),
]

STATUSES = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]

def generate_orders(n=500):
    rows = []
    base_date = datetime(2024, 1, 1)
    for i in range(1, n + 1):
        order_date = base_date + timedelta(days=random.randint(0, 365))
        customer = random.choice(CUSTOMERS)
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 5)
        discount = random.choice([0, 5, 10, 15, 20])
        price = product[3]
        total = round(price * qty * (1 - discount / 100), 2)
        status = random.choices(STATUSES, weights=[50, 20, 15, 10, 5])[0]
        rows.append({
            "order_id": f"ORD{i:05d}",
            "order_date": order_date.strftime("%Y-%m-%d"),
            "customer_id": customer[0],
            "customer_name": customer[1],
            "city": customer[2],
            "state": customer[3],
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "unit_price": price,
            "quantity": qty,
            "discount_pct": discount,
            "total_amount": total,
            "status": status,
        })
    return rows

def generate_products():
    rows = []
    for p in PRODUCTS:
        rows.append({
            "product_id": p[0],
            "product_name": p[1],
            "category": p[2],
            "unit_price": p[3],
            "stock_quantity": random.randint(10, 500),
            "supplier": random.choice(["SupplierA", "SupplierB", "SupplierC"]),
        })
    return rows

def generate_customers():
    rows = []
    for c in CUSTOMERS:
        rows.append({
            "customer_id": c[0],
            "customer_name": c[1],
            "city": c[2],
            "state": c[3],
            "email": c[1].lower().replace(" ", ".") + "@email.com",
            "signup_date": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d"),
        })
    return rows

def write_csv(filename, rows):
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated: {path} ({len(rows)} rows)")

if __name__ == "__main__":
    write_csv("orders.csv", generate_orders(500))
    write_csv("products.csv", generate_products())
    write_csv("customers.csv", generate_customers())
    print("All datasets generated successfully.")
