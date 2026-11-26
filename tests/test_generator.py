import csv
import json
import pytest

from collections import Counter
from decimal import Decimal

from src.generator.generate_data import generate


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_events(path):
    return [
        json.loads(line)
        for line in path.read_text().splitlines()
    ]


@pytest.fixture
def clean(tmp_path):
    output = tmp_path / "clean"
    generate(output)
    return output


def test_row_counts(clean):
    manifest = json.loads(
        (clean / "manifest.json").read_text()
    )

    expected = {
        "customers.csv": 20,
        "products.csv": 50,
        "orders.csv": 100,
        "order_items.csv": 200,
        "events.jsonl": 300,
        "web_logs.txt": 100,
        "ip_country_map.csv": 3,
        "order_cdc.csv": 6
    }

    assert manifest["row_counts"] == expected

    for name, count in expected.items():
        lines = (clean / name).read_text().splitlines()

        assert len(lines) - int(name.endswith(".csv")) == count


def test_primary_keys(clean):
    tables = {
        "customers.csv": ["customer_id"],
        "products.csv": ["product_id"],
        "orders.csv": ["order_id"],
        "order_items.csv": ["order_id", "line_id"]
    }

    for name, columns in tables.items():
        rows = read_csv(clean / name)

        keys = [
            tuple(row[c] for c in columns)
            for row in rows
        ]

        assert len(keys) == len(set(keys))

    events = read_events(clean / "events.jsonl")

    assert len(events) == len({
        e["event_id"] for e in events
    })


def test_foreign_keys(clean):
    customers = {
        r["customer_id"]
        for r in read_csv(clean / "customers.csv")
    }

    products = {
        r["product_id"]
        for r in read_csv(clean / "products.csv")
    }

    orders = read_csv(clean / "orders.csv")
    items = read_csv(clean / "order_items.csv")

    order_ids = {r["order_id"] for r in orders}

    assert all(
        r["customer_id"] in customers
        for r in orders
    )

    assert all(
        r["order_id"] in order_ids
        and r["product_id"] in products
        and int(r["quantity"]) > 0
        and Decimal(r["unit_price"]) > 0
        for r in items
    )


def test_purchase_events(clean):
    orders = {
        r["order_id"]: r
        for r in read_csv(clean / "orders.csv")
    }

    item_keys = {
        (r["order_id"], r["product_id"])
        for r in read_csv(clean / "order_items.csv")
    }

    events = read_events(clean / "events.jsonl")

    for event in events:
        if event["event_type"] == "purchase":
            order = orders[event["order_id"]]

            assert order["status"] == "completed"

            assert order["customer_id"] == event["visitor_id"]

            assert (
                event["order_id"],
                event["product_id"]
            ) in item_keys

        else:
            assert event["order_id"] is None


def test_reproducibility(clean, tmp_path):
    replay = tmp_path / "replay"

    generate(replay, seed=42)

    assert {
        p.name: p.read_bytes()
        for p in clean.iterdir()
    } == {
        p.name: p.read_bytes()
        for p in replay.iterdir()
    }


def test_dirty_data(tmp_path):
    output = tmp_path / "dirty"

    generate(output, variant="dirty")

    orders = read_csv(output / "orders.csv")
    items = read_csv(output / "order_items.csv")

    assert orders[0] == orders[1]

    assert any(r["customer_id"] == "" for r in orders)
    assert any(r["customer_id"] == "C_ORPHAN" for r in orders)
    assert any(r["order_ts"] == "invalid_timestamp" for r in orders)
    assert any(r["status"] == "invalid_status" for r in orders)

    assert items[0] == items[1]

    assert any(r["quantity"] == "-1" for r in items)
    assert any(r["unit_price"] == "bad_price" for r in items)
    assert any(r["product_id"] == "P_ORPHAN" for r in items)
    assert any(r["order_id"] == "O_ORPHAN" for r in items)

    assert len(read_events(output / "events.jsonl")) == 301

    logs = (output / "web_logs.txt").read_text().splitlines()

    assert len(logs) == 102
    assert "INVALID LOG" in logs


def test_skew(tmp_path):
    output = tmp_path / "skew"

    generate(output, variant="skew")

    rows = read_csv(output / "order_items.csv")

    counts = Counter(
        r["product_id"] for r in rows
    )

    assert len(rows) == 200

    assert counts["P000001"] == 140


def test_cdc(clean):
    rows = read_csv(clean / "order_cdc.csv")

    assert rows[0]["order_id"] == rows[1]["order_id"]

    assert int(rows[0]["change_seq"]) > int(
        rows[1]["change_seq"]
    )

    assert {r["op"] for r in rows} == {"I", "U", "D"}


def test_prevent_overwrite(clean):
    with pytest.raises(FileExistsError):
        generate(clean)
