"""
Data generator for the retail data platform.
Generates realistic e-commerce data:
  - 1000 sales transactions
  - 50 customers
  - 20 products
  - 5 product categories

Run: python3 generate_data.py
Output: sales.csv, customers.csv, products.csv
"""

import csv
import random
import json
from datetime import datetime, timedelta

# Seed for reproducibility — same seed always produces same data.
# This is important for a portfolio project so results are consistent.
random.seed(42)

# -------------------------------------------------------
# Reference data — realistic product catalog
# -------------------------------------------------------
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Sports", "Books"]

PRODUCTS = [
    {"product_id": f"P{str(i).zfill(3)}", "name": name, "category": cat, "price": price}
    for i, (name, cat, price) in enumerate([
        ("Wireless Headphones",    "Electronics",    79.99),
        ("Running Shoes",          "Sports",         59.99),
        ("Python Programming",     "Books",          39.99),
        ("Coffee Maker",           "Home & Kitchen", 49.99),
        ("Yoga Mat",               "Sports",         29.99),
        ("Bluetooth Speaker",      "Electronics",    49.99),
        ("Winter Jacket",          "Clothing",       89.99),
        ("Air Fryer",              "Home & Kitchen", 69.99),
        ("Data Science Handbook",  "Books",          44.99),
        ("Smart Watch",            "Electronics",   199.99),
        ("Running Shorts",         "Sports",         24.99),
        ("Desk Lamp",              "Home & Kitchen", 34.99),
        ("Novel: The Algorithm",   "Books",          14.99),
        ("Protein Powder",         "Sports",         54.99),
        ("Laptop Stand",           "Electronics",    39.99),
        ("Linen Shirt",            "Clothing",       34.99),
        ("French Press",           "Home & Kitchen", 27.99),
        ("Kindle E-Reader",        "Electronics",   129.99),
        ("Hiking Boots",           "Sports",         99.99),
        ("Mechanical Keyboard",    "Electronics",   149.99),
    ], start=1)
]

STATUSES   = ["completed", "completed", "completed", "pending", "cancelled"]
# completed is weighted 3x — mirrors real e-commerce where most orders complete

CITIES = [
    "Nairobi", "Lagos", "Accra", "Johannesburg", "Cairo",
    "London",  "Paris", "Berlin", "Amsterdam",   "Madrid"
]

# -------------------------------------------------------
# Generate customers
# -------------------------------------------------------
def generate_customers(n=50):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id"   : f"C{str(i).zfill(3)}",
            "first_name"    : random.choice([
                "James", "Mary", "John", "Patricia", "Robert",
                "Jennifer", "Michael", "Linda", "William", "Barbara",
                "David", "Susan", "Amara", "Kwame", "Fatima",
                "Ibrahim", "Aisha", "Kofi", "Nadia", "Omar"
            ]),
            "last_name"     : random.choice([
                "Smith", "Johnson", "Williams", "Brown", "Jones",
                "Garcia", "Miller", "Davis", "Osei", "Mensah",
                "Diallo", "Nkrumah", "Kamau", "Mwangi", "Abubakar"
            ]),
            "email"         : f"customer{i}@email.com",
            "city"          : random.choice(CITIES),
            "signup_date"   : (
                datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d"),
            "loyalty_tier"  : random.choice(["bronze", "bronze", "silver", "gold"])
            # bronze weighted 2x — most customers are new
        })
    return customers

# -------------------------------------------------------
# Generate sales transactions
# -------------------------------------------------------
def generate_sales(customers, products, n=1000):
    sales = []
    start_date = datetime(2024, 1, 1)

    for i in range(1, n + 1):
        customer = random.choice(customers)
        product  = random.choice(products)
        quantity = random.randint(1, 5)
        discount = random.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15])
        # discount is 0 most of the time — realistic

        unit_price    = product["price"]
        gross_amount  = round(unit_price * quantity, 2)
        discount_amt  = round(gross_amount * discount, 2)
        net_amount    = round(gross_amount - discount_amt, 2)

        order_date = start_date + timedelta(days=random.randint(0, 364))

        sales.append({
            "order_id"      : f"ORD{str(i).zfill(5)}",
            "customer_id"   : customer["customer_id"],
            "product_id"    : product["product_id"],
            "product_name"  : product["name"],
            "category"      : product["category"],
            "quantity"      : quantity,
            "unit_price"    : unit_price,
            "discount_pct"  : discount,
            "gross_amount"  : gross_amount,
            "discount_amt"  : discount_amt,
            "net_amount"    : net_amount,
            "order_date"    : order_date.strftime("%Y-%m-%d"),
            "status"        : random.choice(STATUSES),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "mpesa"]),
            "city"          : customer["city"]
        })
    return sales

# -------------------------------------------------------
# Write to CSV
# -------------------------------------------------------
def write_csv(data, filename):
    if not data:
        return
    filepath = f"{filename}.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Generated {len(data)} rows → {filepath}")

# -------------------------------------------------------
# Main
# -------------------------------------------------------
customers = generate_customers(50)
products  = PRODUCTS
sales     = generate_sales(customers, products, 1000)

write_csv(customers, "customers")
write_csv(products,  "products")
write_csv(sales,     "sales")

print("Done. Files ready for upload to S3.")
