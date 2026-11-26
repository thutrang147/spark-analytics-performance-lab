import argparse
import json

from pathlib import Path
from time import perf_counter
from statistics import median
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================
# 1. SCHEMA DEFINITION
# =========================================

CSV_SCHEMAS = {
    "customers": """
        customer_id STRING,
        country STRING,
        signup_date STRING
    """,

    "products": """
        product_id STRING,
        category STRING,
        unit_price STRING
    """,

    "orders": """
        order_id STRING,
        customer_id STRING,
        order_ts STRING,
        status STRING
    """,

    "order_items": """
        order_id STRING,
        line_id STRING,
        product_id STRING,
        quantity STRING,
        unit_price STRING
    """,

    "ip_country_map": """
        ip_prefix STRING,
        country STRING
    """,

    "order_cdc": """
        order_id STRING,
        status STRING,
        updated_at STRING,
        change_seq STRING,
        op STRING
    """
}


EVENT_SCHEMA = """
    event_id STRING,
    event_time STRING,
    visitor_id STRING,
    product_id STRING,
    event_type STRING,
    order_id STRING,
    metadata STRUCT<
        device: STRUCT<
            type: STRING,
            os: STRING
        >,
        tags: ARRAY<STRING>,
        attributes: MAP<STRING, STRING>
    >
"""


# =========================================
# 2. SPARK JOB TRACKING
# =========================================

@contextmanager
def job_group(sc, group, description):

    sc.setJobGroup(group, description)

    try:
        yield

    finally:
        for key in (
            "spark.jobGroup.id",
            "spark.job.description",
            "spark.job.interruptOnCancel"
        ):
            sc.setLocalProperty(key, None)


# =========================================
# 3. READ RAW DATA
# =========================================

def read_raw(spark, raw_dir):

    raw_dir = Path(raw_dir)

    tables = {}

    for name, schema in CSV_SCHEMAS.items():

        tables[name] = (
            spark.read
            .option("header", True)
            .option("enforceSchema", False)
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(str(raw_dir / f"{name}.csv"))
        )

    tables["events"] = (
        spark.read
        .option("mode", "FAILFAST")
        .schema(EVENT_SCHEMA)
        .json(str(raw_dir / "events.jsonl"))
    )

    return tables


# =========================================
# 4. INGESTION PIPELINE
# =========================================

def run_ingestion(
    spark,
    raw_dir,
    output_dir,
    overwrite=False
):

    sc = spark.sparkContext

    raw_dir = Path(raw_dir).resolve()
    output_dir = Path(output_dir).resolve()

    # Protect source data
    if (
        raw_dir == output_dir
        or raw_dir in output_dir.parents
        or output_dir in raw_dir.parents
    ):
        raise ValueError(
            "Raw and staging paths must be separate"
        )

    if (
        output_dir.exists()
        and any(output_dir.iterdir())
        and not overwrite
    ):
        raise FileExistsError(
            f"{output_dir} already contains data"
        )

    manifest = json.loads(
        (raw_dir / "manifest.json").read_text()
    )

    expected_counts = manifest["row_counts"]

    with job_group(
        sc, "read_raw", "Read source metadata"
    ):
        tables = read_raw(spark, raw_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    actual_counts = {}

    for name, df in tables.items():

        path = str(output_dir / name)

        print(f"\nProcessing: {name}")

        with job_group(
            sc,
            f"ingest_{name}",
            f"Ingest {name} to Parquet"
        ):

            df.write.mode("overwrite").parquet(path)

        with job_group(
            sc,
            f"validate_{name}",
            f"Validate staged {name}"
        ):

            staged = spark.read.parquet(path)
            actual = staged.count()

        extension = (
            "jsonl" if name == "events" else "csv"
        )

        expected = expected_counts[
            f"{name}.{extension}"
        ]

        if actual != expected:
            raise ValueError(
                f"{name}: expected={expected}, "
                f"actual={actual}"
            )

        actual_counts[name] = actual

        print(f"PASS: {actual:,} rows")

    print("\nINGESTION COMPLETED")
    print(json.dumps(actual_counts, indent=2))

    return actual_counts


# =========================================
# 5. BENCHMARK SCHEMA INFERENCE
# =========================================

def benchmark_schema(spark, raw_dir, repeat=3):

    sc = spark.sparkContext
    tracker = sc.statusTracker()

    path = str(Path(raw_dir) / "orders.csv")

    def infer():
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )

    # Use the same schema for a fair comparison
    reference = infer()
    reference_schema = reference.schema

    def explicit():
        return (
            spark.read
            .option("header", True)
            .schema(reference_schema)
            .csv(path)
        )

    # Warm-up
    infer()
    explicit()

    methods = {
        "infer": infer,
        "explicit": explicit
    }

    timings = {
        "infer": [],
        "explicit": []
    }

    for i in range(repeat):

        # Alternate execution order
        names = (
            ["infer", "explicit"]
            if i % 2 == 0
            else ["explicit", "infer"]
        )

        for name in names:

            group = f"schema_{name}_{i}"

            with job_group(sc, group, group):

                start = perf_counter()

                df = methods[name]()

                elapsed = perf_counter() - start

            assert df.schema == reference_schema

            timings[name].append(elapsed)

            jobs = tracker.getJobIdsForGroup(group)

            print(
                f"{group}: "
                f"{elapsed:.4f}s | "
                f"jobs={len(jobs)} | "
                f"ids={jobs}"
            )

    infer_median = median(timings["infer"])

    explicit_median = median(
        timings["explicit"]
    )

    print("\nSCHEMA BENCHMARK")

    print(
        f"Infer median: {infer_median:.4f}s"
    )

    print(
        f"Explicit median: {explicit_median:.4f}s"
    )

    print(
        f"Speedup: "
        f"{infer_median / explicit_median:.2f}x"
    )


# =========================================
# 6. INSPECT EXECUTION PLAN
# =========================================

def inspect_plan(spark, output_dir):

    sc = spark.sparkContext

    orders = spark.read.parquet(
        str(Path(output_dir) / "orders")
    )

    group = "build_query"

    with job_group(sc, group, "Build lazy query"):

        query = (
            orders
            .filter(F.col("status") == "completed")
            .select("order_id", "customer_id")
        )

    jobs = sc.statusTracker().getJobIdsForGroup(group)

    print("\nJOBS CREATED BY TRANSFORMATIONS:", jobs)

    print("\nEXECUTION PLAN")

    query.explain("formatted")

    with job_group(
        sc,
        "execute_query",
        "Execute completed orders query"
    ):
        query.show(5)


# =========================================
# 7. COMMAND LINE INTERFACE
# =========================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument(
        "--overwrite", action="store_true"
    )

    parser.add_argument(
        "--benchmark-schema", action="store_true"
    )

    parser.add_argument(
        "--hold-ui", action="store_true"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Ecommerce-Ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    sc = spark.sparkContext

    sc.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print("Master:", sc.master)
    print("Spark UI:", sc.uiWebUrl)

    try:

        if args.benchmark_schema:
            benchmark_schema(spark, args.input)

        run_ingestion(
            spark,
            args.input,
            args.output,
            args.overwrite
        )

        inspect_plan(spark, args.output)

        if args.hold_ui:
            input("Press Enter to stop Spark...")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
