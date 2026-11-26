import pytest

from pyspark.sql import SparkSession

from src.generator.generate_data import generate
from src.ingestion.process_web_logs import (
    parse_log,
    run_web_logs
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("Test-Web-Logs")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


def test_valid_log():
    record, reason = parse_log(
        "10.1.1.10 2026-01-01T09:00:00 GET /products 200"
    )

    assert reason is None
    assert record[0] == "10.1.1.10"
    assert record[4] == 200


def test_malformed_log():
    record, reason = parse_log("INVALID LOG")

    assert record is None
    assert reason == "INVALID_FIELD_COUNT"


def test_invalid_timestamp():
    record, reason = parse_log(
        "10.1.1.10 broken_timestamp GET /home 200"
    )

    assert record is None
    assert reason == "INVALID_TIMESTAMP"


def test_invalid_ip():
    record, reason = parse_log(
        "999.1.1.10 2026-01-01T09:00:00 GET /home 200"
    )

    assert record is None
    assert reason == "INVALID_IP"


def test_http_error_is_valid():
    record, reason = parse_log(
        "10.1.1.10 2026-01-01T09:00:00 POST /orders 500"
    )

    assert reason is None
    assert record[4] == 500


@pytest.mark.parametrize(
    "variant, expected_total, expected_rejected",
    [
        ("clean", 100, 0),
        ("dirty", 102, 2)
    ]
)
def test_pipeline(
    spark,
    tmp_path,
    variant,
    expected_total,
    expected_rejected
):
    raw = tmp_path / "raw"
    staging = tmp_path / "staging"

    generate(raw, variant=variant)

    result = run_web_logs(spark, raw, staging)

    assert result["total"] == expected_total
    assert result["valid"] == 100
    assert result["rejected"] == expected_rejected
    assert sum(result["country_summary"].values()) == 100

    valid = spark.read.parquet(
        str(staging / "web_logs")
    )

    rejected = spark.read.parquet(
        str(staging / "web_logs_rejected")
    )

    summary = spark.read.parquet(
        str(staging / "web_log_country_summary")
    )

    assert valid.count() == 100
    assert rejected.count() == expected_rejected

    actual_summary = {
        row.country: row.request_count
        for row in summary.collect()
    }

    expected_summary = {
        row.country: row["count"]
        for row in valid.groupBy("country").count().collect()
    }

    assert actual_summary == expected_summary

    if variant == "dirty":
        reasons = {
            row.reject_reason
            for row in rejected.collect()
        }

        assert reasons == {
            "INVALID_FIELD_COUNT",
            "INVALID_TIMESTAMP"
        }
