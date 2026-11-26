import argparse
import csv
import json
import random

from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal


# =========================================
# 1. CONFIGURATION
# =========================================

SIZES = {
    "tiny": (100, 20, 50),
    "standard": (100_000, 20_000, 1_000),
    "stress": (1_000_000, 200_000, 10_000)
}

COUNTRIES = ("Vietnam", "Japan", "Singapore")

CATEGORIES = (
    "Electronics", "Fashion", "Home",
    "Beauty", "Sports"
)

START = datetime(2026, 1, 1)


# =========================================
# 2. HELPER FUNCTIONS
# =========================================

def oid(i):
    return f"O{i:09d}"


def cid(i):
    return f"C{i:07d}"


def pid(i):
    return f"P{i:06d}"


def timestamp(i):
    return (
        START + timedelta(minutes=i)
    ).isoformat(sep=" ")


def price(i):
    return Decimal(
        1000 + (i * 7919) % 499_000
    ) / Decimal(100)


def product_for(index, n_products, variant):
    if variant == "skew":
        if index % 10 < 7:
            return 1

        return 2 + index % (n_products - 1)

    return 1 + index % n_products


def write_csv(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        count = 0

        for row in rows:
            writer.writerow(row)
            count += 1

    return count


def write_lines(path, rows):
    with path.open("w", encoding="utf-8") as f:
        count = 0

        for row in rows:
            f.write(row + "\n")
            count += 1

    return count


# =========================================
# 3. CUSTOMERS
# =========================================

def customers(n, seed, variant):
    rng = random.Random(seed + 1)

    for i in range(1, n + 1):
        row = (
            cid(i),
            rng.choice(COUNTRIES),
            (
                START.date()
                - timedelta(days=rng.randint(1, 730))
            ).isoformat()
        )

        yield row

        if variant == "dirty" and i == 1:
            yield row


# =========================================
# 4. PRODUCTS
# =========================================

def products(n, variant):
    for i in range(1, n + 1):
        row = (
            pid(i),
            CATEGORIES[(i - 1) % len(CATEGORIES)],
            f"{price(i):.2f}"
        )

        yield row

        if variant == "dirty" and i == 1:
            yield row


# =========================================
# 5. ORDERS
# =========================================

def orders(n, n_customers, seed, variant):
    rng = random.Random(seed + 2)

    for i in range(1, n + 1):
        customer = cid(rng.randint(1, n_customers))

        status = rng.choices(
            ["completed", "cancelled", "pending"],
            weights=[75, 15, 10]
        )[0]

        ts = timestamp(i)

        if variant == "dirty":
            if i == 2:
                customer = ""
            if i == 3:
                customer = "C_ORPHAN"
            if i == 4:
                ts = "invalid_timestamp"
            if i == 5:
                status = "invalid_status"

        row = (oid(i), customer, ts, status)

        yield row

        if variant == "dirty" and i == 1:
            yield row


# =========================================
# 6. ORDER ITEMS
# =========================================

def order_items(n, n_products, seed, variant):
    rng = random.Random(seed + 3)

    for i in range(1, n + 1):
        for line_id in (1, 2):
            index = (i - 1) * 2 + line_id - 1

            product = product_for(
                index, n_products, variant
            )

            order_id = oid(i)
            product_id = pid(product)
            quantity = rng.randint(1, 5)
            unit_price = f"{price(product):.2f}"

            if variant == "dirty":
                if index == 2:
                    quantity = -1
                if index == 3:
                    product_id = "P_ORPHAN"
                if index == 4:
                    order_id = "O_ORPHAN"
                if index == 5:
                    unit_price = "bad_price"

            row = (
                order_id, line_id, product_id,
                quantity, unit_price
            )

            yield row

            if variant == "dirty" and index == 0:
                yield row


# =========================================
# 7. USER EVENTS
# =========================================

def events(n, n_customers, n_products, seed, variant):

    base_orders = orders(
        n, n_customers, seed, "clean"
    )

    for i, (order_id, visitor, _, status) in enumerate(
        base_orders, start=1
    ):

        product = pid(
            product_for(
                (i - 1) * 2,
                n_products,
                variant
            )
        )

        event_types = [
            "view",
            "cart",
            "purchase" if status == "completed" else "view"
        ]

        for j, event_type in enumerate(event_types):
            index = (i - 1) * 3 + j + 1

            event = {
                "event_id": f"E{index:010d}",
                "event_time": (
                    START + timedelta(
                        minutes=i,
                        seconds=(j - 2) * 10
                    )
                ).isoformat(sep=" "),
                "visitor_id": visitor,
                "product_id": product,
                "event_type": event_type,
                "order_id": (
                    order_id
                    if event_type == "purchase"
                    else None
                ),
                "metadata": {
                    "device": {
                        "type": "mobile" if i % 2 else "desktop",
                        "os": "iOS" if i % 2 else "macOS"
                    },
                    "tags": ["organic", "ecommerce"],
                    "attributes": {
                        "source": "web",
                        "campaign": "synthetic"
                    }
                }
            }

            if variant == "dirty":
                if index == 2:
                    event["event_time"] = "bad_timestamp"
                if index == 3:
                    event["product_id"] = "P_ORPHAN"
                if index == 4:
                    event["event_type"] = "invalid_event"

            line = json.dumps(
                event,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":")
            )

            yield line

            if variant == "dirty" and index == 1:
                yield line


# =========================================
# 8. WEB LOGS
# =========================================

def web_logs(n, seed, variant):
    rng = random.Random(seed + 4)

    routes = [
        ("GET", "/products", 200),
        ("GET", "/home", 200),
        ("POST", "/cart", 201),
        ("POST", "/orders", 201),
        ("GET", "/unknown", 404),
        ("POST", "/orders", 500)
    ]

    for i in range(1, n + 1):
        prefix = rng.choices(
            [1, 2, 3, 9],
            weights=[40, 30, 20, 10]
        )[0]

        method, endpoint, status = rng.choices(
            routes,
            weights=[35, 20, 20, 20, 3, 2]
        )[0]

        ip = (
            f"10.{prefix}."
            f"{(i // 250) % 256}.{i % 250 + 1}"
        )

        ts = timestamp(i).replace(" ", "T")

        yield f"{ip} {ts} {method} {endpoint} {status}"

    if variant == "dirty":
        yield "INVALID LOG"
        yield "10.1.1.14 broken_timestamp GET /home ABC"


# =========================================
# 9. CHANGE DATA CAPTURE
# =========================================

def cdc(n):
    return [
        (oid(1), "completed", "2028-01-01 00:02:00", 2, "U"),
        (oid(1), "pending", "2028-01-01 00:01:00", 1, "U"),
        (oid(2), "", "2028-01-01 00:02:00", 2, "D"),
        (oid(2), "completed", "2028-01-01 00:01:00", 1, "U"),
        (oid(n + 1), "pending", "2028-01-01 00:01:00", 1, "I"),
        (oid(n + 1), "completed", "2028-01-01 00:03:00", 2, "U")
    ]


# =========================================
# 10. GENERATION PIPELINE
# =========================================

def generate(
    output,
    size="tiny",
    variant="clean",
    seed=42,
    overwrite=False
):
    if size not in SIZES:
        raise ValueError("Invalid size")

    if variant not in ("clean", "dirty", "skew"):
        raise ValueError("Invalid variant")

    output = Path(output)

    if output.exists() and any(output.iterdir()) and not overwrite:
        raise FileExistsError(
            f"{output} is not empty. Use --overwrite."
        )

    output.mkdir(parents=True, exist_ok=True)

    manifest_path = output / "manifest.json"

    if manifest_path.exists():
        manifest_path.unlink()

    n, c, p = SIZES[size]

    counts = {}

    counts["customers.csv"] = write_csv(
        output / "customers.csv",
        ["customer_id", "country", "signup_date"],
        customers(c, seed, variant)
    )

    counts["products.csv"] = write_csv(
        output / "products.csv",
        ["product_id", "category", "unit_price"],
        products(p, variant)
    )

    counts["orders.csv"] = write_csv(
        output / "orders.csv",
        ["order_id", "customer_id", "order_ts", "status"],
        orders(n, c, seed, variant)
    )

    counts["order_items.csv"] = write_csv(
        output / "order_items.csv",
        [
            "order_id", "line_id", "product_id",
            "quantity", "unit_price"
        ],
        order_items(n, p, seed, variant)
    )

    counts["events.jsonl"] = write_lines(
        output / "events.jsonl",
        events(n, c, p, seed, variant)
    )

    counts["web_logs.txt"] = write_lines(
        output / "web_logs.txt",
        web_logs(n, seed, variant)
    )

    counts["ip_country_map.csv"] = write_csv(
        output / "ip_country_map.csv",
        ["ip_prefix", "country"],
        zip(["10.1", "10.2", "10.3"], COUNTRIES)
    )

    counts["order_cdc.csv"] = write_csv(
        output / "order_cdc.csv",
        ["order_id", "status", "updated_at", "change_seq", "op"],
        cdc(n)
    )

    manifest = {
        "size": size,
        "variant": variant,
        "seed": seed,
        "row_counts": counts,
        "generator_version": 1
    }

    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8"
    )

    return manifest


# =========================================
# 11. COMMAND LINE INTERFACE
# =========================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--size",
        choices=SIZES,
        default="tiny"
    )

    parser.add_argument(
        "--variant",
        choices=["clean", "dirty", "skew"],
        default="clean"
    )

    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    output = args.output or (
        Path("data/raw") / f"{args.size}_{args.variant}"
    )

    manifest = generate(
        output,
        args.size,
        args.variant,
        args.seed,
        args.overwrite
    )

    print("Output:", output.resolve())
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
