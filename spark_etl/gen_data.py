#!/usr/bin/env python3
import argparse, csv, os, random, time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import numpy as np
except ImportError:
    np = None

def rand_name():
    first = ["Aarav","Aarya","Alisha","Bikash","Bibek","Kiran","Niraj","Pratik","Ramesh","Sita","Sunita","Umesh","Zoya"]
    last  = ["Karki","Shrestha","Gurung","Rai","Thapa","Singh","Maharjan","Tamrakar","Adhikari","Shah"]
    return random.choice(first) + " " + random.choice(last)

def rand_email(name):
    base = name.lower().replace(" ", ".")
    doms = ["example.com","mail.com","sample.org"]
    return f"{base}{random.randint(1,999)}@{random.choice(doms)}"

def rand_product(idx):
    cats = [("Electronics", ["Phone","Laptop","Tablet","Headphones","Smartwatch"]),
            ("Home", ["Mixer","Vacuum","Iron","AirPurifier"]),
            ("Beauty", ["Cream","Serum","Shampoo","Conditioner"]),
            ("Grocery", ["Rice","Lentils","Oil","Sugar","Tea"])]
    cat, items = random.choice(cats)
    name = random.choice(items)
    price = round(random.uniform(5, 1500), 2)
    return idx, name, cat, price

def daterange(days_back=180):
    end = datetime.now().date()
    start = end - timedelta(days=days_back)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def ensure_dir(p): Path(p).mkdir(parents=True, exist_ok=True)

def make_customers(path, n_customers, seed):
    random.seed(seed)
    if np is not None:
        np.random.seed(seed)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["customer_id","name","email","country"])
        for cid in range(1, n_customers+1):
            name = rand_name()
            w.writerow([cid, name, rand_email(name), "NP"])
    return n_customers

def make_products(path, n_products, seed):
    random.seed(seed+7)
    if np is not None:
        np.random.seed(seed+7)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id","product_name","category","unit_price"])
        for pid in range(1, n_products+1):
            p = rand_product(pid)
            w.writerow(p)
    return n_products

def zipf_customer(n_customers, skew):
    """Return 1..n_customers with head-heavy distribution, without collapsing to one ID."""
    if skew <= 1.0:
        return random.randint(1, n_customers)

    # Preferred: NumPy Zipf with rejection
    if np is not None:
        for _ in range(20):  # try a few times to get within range
            k = int(np.random.zipf(skew))
            if 1 <= k <= n_customers:
                return k
        # fallback to uniform if repeated overflow
        return random.randint(1, n_customers)

    # Fallback without NumPy: piecewise head + uniform tail
    # 85% from head bucket, 15% uniform
    if random.random() < 0.85:
        # geometric-like head: small ids more likely, but bounded
        span = max(10, n_customers // 50)
        return 1 + int(random.random() ** (1.0/skew) * (span - 1))
    else:
        return random.randint(1, n_customers)

def make_orders(path, n_orders, n_customers, n_products, max_qty, skew, seed, chunk=200_000):
    random.seed(seed+13)
    if np is not None:
        np.random.seed(seed+13)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id","customer_id","product_id","quantity","unit_price","order_date"])
        oid = 1
        written = 0
        t0 = time.time()
        while written < n_orders:
            to_write = min(chunk, n_orders - written)
            rows = []
            for _ in range(to_write):
                cid = zipf_customer(n_customers, skew)
                pid = random.randint(1, n_products)
                qty = random.randint(1, max_qty)
                base = 5 + (pid % 50) * 10
                price = round(random.uniform(base*0.8, base*1.2), 2)
                dt = daterange()
                rows.append([oid, cid, pid, qty, price, dt.isoformat()])
                oid += 1
            w.writerows(rows)
            written += to_write
            if written % 1_000_000 == 0 or written == n_orders:
                print(f"[orders] {written}/{n_orders} rows written in {time.time()-t0:.1f}s")
    return n_orders

def main():
    ap = argparse.ArgumentParser(description="Generate large CSVs for PySpark ETL demo")
    ap.add_argument("--outdir", default="data", help="output directory")
    ap.add_argument("--customers", type=int, default=200_000)
    ap.add_argument("--products", type=int, default=3_000)
    ap.add_argument("--orders", type=int, default=5_000_000)
    ap.add_argument("--max-qty", type=int, default=5)
    ap.add_argument("--skew", type=float, default=1.6, help=">1.0 increases head bias")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    ensure_dir(args.outdir)
    cpath = os.path.join(args.outdir, "customers.csv")
    ppath = os.path.join(args.outdir, "products.csv")
    opath = os.path.join(args.outdir, "orders.csv")

    print("Generating customers...")
    make_customers(cpath, args.customers, args.seed)
    print("Generating products...")
    make_products(ppath, args.products, args.seed)
    print("Generating orders...")
    make_orders(opath, args.orders, args.customers, args.products, args.max_qty, args.skew, args.seed)
    print("Done.")

if __name__ == "__main__":
    main()
