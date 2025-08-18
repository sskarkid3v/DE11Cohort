from dateutil.parser import isoparse

def clean_rows(row):
    cleaned = []
    for r in row:
        cleaned.append({
            "order_id": str(r["order_id"]),
            "order_date": str(r["order_date"]),
            "customer_id": str(r["customer_id"]),
            "amount": float(r["amount"]),
            "updated_at": isoparse(r["updated_at"]).isoformat()
        })
    return cleaned