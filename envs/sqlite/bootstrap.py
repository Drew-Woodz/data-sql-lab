import sqlite3, csv, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "envs" / "sqlite" / "tiny.db"
DATA_DIR = ROOT / "datasets" / "tiny"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.executescript("""
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

CREATE TABLE products (
  product_id   INTEGER PRIMARY KEY,
  product_name TEXT NOT NULL,
  unit_price   REAL NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE customers (
  customer_id   INTEGER PRIMARY KEY,
  customer_name TEXT NOT NULL,
  region        TEXT
);

CREATE TABLE orders (
  order_id    INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  order_date  TEXT NOT NULL,
  FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  order_item_id INTEGER PRIMARY KEY,
  order_id      INTEGER NOT NULL,
  product_id    INTEGER NOT NULL,
  quantity      INTEGER NOT NULL CHECK (quantity > 0),
  FOREIGN KEY(order_id)  REFERENCES orders(order_id),
  FOREIGN KEY(product_id) REFERENCES products(product_id)
);
""")

def cast_row(table: str, row: dict):
    """Return a tuple of values in the correct order and types for each table."""
    if table == "products":
        return (
            int(row["product_id"]),
            row["product_name"].strip(),
            float(row["unit_price"]),
        )
    if table == "customers":
        return (
            int(row["customer_id"]),
            row["customer_name"].strip(),
            (row["region"] or "").strip(),
        )
    if table == "orders":
        return (
            int(row["order_id"]),
            int(row["customer_id"]),
            row["order_date"].strip(),  # keep as TEXT
        )
    if table == "order_items":
        return (
            int(row["order_item_id"]),
            int(row["order_id"]),
            int(row["product_id"]),
            int(row["quantity"]),
        )
    raise ValueError(f"Unknown table {table}")

def load_csv(table, columns):
    with open(DATA_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [cast_row(table, r) for r in reader]
    placeholders = ",".join("?" * len(columns))
    cur.executemany(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})", rows
    )

load_csv("products", ["product_id","product_name","unit_price"])
load_csv("customers", ["customer_id","customer_name","region"])
load_csv("orders", ["order_id","customer_id","order_date"])
load_csv("order_items", ["order_item_id","order_id","product_id","quantity"])

conn.commit()
conn.close()
print(f"Built {DB_PATH}")
