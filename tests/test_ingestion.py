import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.generator.generate_data import generate
from src.ingestion.read_raw import read_raw, run_ingestion


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("Test-Ingestion")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def datasets(tmp_path_factory):
    root = tmp_path_factory.mktemp("ingestion")

    clean = root / "clean"
    dirty = root / "dirty"

    generate(clean, variant="clean")
    generate(dirty, variant="dirty")

    return clean, dirty


def test_table_count(spark, datasets):
    tables = read_raw(spark, datasets[0])
    assert len(tables) == 7


def test_orders_count(spark, datasets):
    tables = read_raw(spark, datasets[0])
    assert tables["orders"].count() == 100


def test_items_count(spark, datasets):
    tables = read_raw(spark, datasets[0])
    assert tables["order_items"].count() == 200


def test_preserve_invalid_price(spark, datasets):
    tables = read_raw(spark, datasets[1])

    assert tables["order_items"].filter(
        F.col("unit_price") == "bad_price"
    ).count() == 1


def test_preserve_invalid_timestamp(spark, datasets):
    tables = read_raw(spark, datasets[1])

    assert tables["orders"].filter(
        F.col("order_ts") == "invalid_timestamp"
    ).count() == 1


def test_nested_metadata(spark, datasets):
    tables = read_raw(spark, datasets[0])

    event = tables["events"].select(
        F.col("metadata.device.type").alias("device"),
        F.col("metadata.tags").alias("tags"),
        F.col("metadata.attributes").alias("attributes")
    ).first()

    assert event.device in ["mobile", "desktop"]
    assert "organic" in event.tags
    assert event.attributes["source"] == "web"


def test_ingestion_pipeline(spark, datasets, tmp_path):
    output = tmp_path / "staging"

    counts = run_ingestion(
        spark, datasets[1], output
    )

    assert counts == {
        "customers": 21,
        "products": 51,
        "orders": 101,
        "order_items": 201,
        "events": 301,
        "ip_country_map": 3,
        "order_cdc": 6
    }

    staged_items = spark.read.parquet(
        str(output / "order_items")
    )

    assert staged_items.filter(
        F.col("unit_price") == "bad_price"
    ).count() == 1
