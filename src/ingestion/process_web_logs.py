import argparse
import csv
import json

from pathlib import Path
from datetime import datetime
from ipaddress import IPv4Address
from time import perf_counter
from statistics import median
from contextlib import contextmanager

from pyspark import StorageLevel
from pyspark.sql import SparkSession


# =========================================
# 1. OUTPUT SCHEMAS
# =========================================

LOG_SCHEMA = """
    ip STRING,
    event_time STRING,
    method STRING,
    endpoint STRING,
    status INT,
    country STRING
"""

REJECT_SCHEMA = """
    raw_line STRING,
    reject_reason STRING
"""

SUMMARY_SCHEMA = """
    country STRING,
    request_count LONG
"""

METHODS = {
    "GET", "POST", "PUT", "DELETE",
    "PATCH", "HEAD", "OPTIONS"
}


# =========================================
# 2. JOB TRACKING
# =========================================

@contextmanager
def job_group(sc, name):

    sc.setJobGroup(name, name)

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
# 3. PARSING AND VALIDATION
# =========================================

def parse_log(line):

    parts = line.split()

    if len(parts) != 5:
        return None, "INVALID_FIELD_COUNT"

    ip, event_time, method, endpoint, status = parts

    try:
        IPv4Address(ip)
    except ValueError:
        return None, "INVALID_IP"

    try:
        datetime.strptime(
            event_time,
            "%Y-%m-%dT%H:%M:%S"
        )
    except ValueError:
        return None, "INVALID_TIMESTAMP"

    if method not in METHODS:
        return None, "INVALID_METHOD"

    if not endpoint.startswith("/"):
        return None, "INVALID_ENDPOINT"

    try:
        status = int(status)
    except ValueError:
        return None, "INVALID_STATUS_FORMAT"

    if not 100 <= status <= 599:
        return None, "INVALID_STATUS_RANGE"

    return (
        ip,
        event_time,
        method,
        endpoint,
        status
    ), None


# =========================================
# 4. LOAD LOOKUP TABLE
# =========================================

def load_country_map(path):

    with Path(path).open(
        encoding="utf-8",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        return {
            row["ip_prefix"]: row["country"]
            for row in reader
        }


# =========================================
# 5. BENCHMARK RDD AGGREGATION
# =========================================

def benchmark_aggregation(pairs, repeat=3):

    sc = pairs.context

    pairs.persist(StorageLevel.MEMORY_AND_DISK)
    pairs.count()

    def run_group():
        return dict(
            pairs.groupByKey(4)
            .mapValues(sum)
            .collect()
        )

    def run_reduce():
        return dict(
            pairs.reduceByKey(
                lambda a, b: a + b,
                numPartitions=4
            ).collect()
        )

    methods = {
        "groupByKey": run_group,
        "reduceByKey": run_reduce
    }

    timings = {
        "groupByKey": [],
        "reduceByKey": []
    }

    try:

        # Warm-up and correctness
        expected = run_group()

        assert expected == run_reduce()

        for i in range(repeat):

            names = (
                ["groupByKey", "reduceByKey"]
                if i % 2 == 0
                else ["reduceByKey", "groupByKey"]
            )

            for name in names:

                group = f"bench_{name}_{i}"

                with job_group(sc, group):

                    start = perf_counter()

                    result = methods[name]()

                    elapsed = perf_counter() - start

                assert result == expected

                timings[name].append(elapsed)

                print(f"{group}: {elapsed:.4f}s")

        group_time = median(timings["groupByKey"])
        reduce_time = median(timings["reduceByKey"])

        report = {
            "runs": timings,
            "groupByKey_median": group_time,
            "reduceByKey_median": reduce_time,
            "speedup": group_time / reduce_time
        }

        print("\nAGGREGATION BENCHMARK")
        print(json.dumps(report, indent=2))

        return report

    finally:
        pairs.unpersist(blocking=True)


# =========================================
# 6. WEB LOG PIPELINE
# =========================================

def run_web_logs(
    spark,
    raw_dir,
    output_dir,
    overwrite=False,
    benchmark=False
):

    sc = spark.sparkContext

    raw_dir = Path(raw_dir).resolve()
    output_dir = Path(output_dir).resolve()

    if (
        raw_dir == output_dir
        or raw_dir in output_dir.parents
        or output_dir in raw_dir.parents
    ):
        raise ValueError(
            "Raw and staging paths must be separate"
        )

    output_names = (
        "web_logs",
        "web_logs_rejected",
        "web_log_country_summary"
    )

    for name in output_names:
        if (output_dir / name).exists() and not overwrite:
            raise FileExistsError(
                f"{output_dir / name} already exists"
            )

    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(
        (raw_dir / "manifest.json").read_text()
    )

    expected_count = manifest["row_counts"]["web_logs.txt"]

    # Broadcast lookup dictionary
    country_map = load_country_map(
        raw_dir / "ip_country_map.csv"
    )

    country_bc = sc.broadcast(country_map)

    # Monitoring counter
    invalid_acc = sc.accumulator(0)

    # Read distributed source
    raw_logs = sc.textFile(
        str(raw_dir / "web_logs.txt"),
        minPartitions=4
    )

    def process(line):

        record, reason = parse_log(line)

        if record is None:

            invalid_acc.add(1)

            return (
                False,
                (line, reason)
            )

        ip = record[0]

        prefix = ".".join(ip.split(".")[:2])

        country = country_bc.value.get(
            prefix,
            "Unknown"
        )

        return (
            True,
            record + (country,)
        )

    # Parse once, reuse across actions
    parsed = raw_logs.map(process).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:

        with job_group(sc, "parse_and_cache_logs"):
            total_count = parsed.count()

        monitored_invalid = invalid_acc.value

        if total_count != expected_count:
            raise ValueError(
                "Raw log count does not match manifest"
            )

        valid = (
            parsed
            .filter(lambda x: x[0])
            .map(lambda x: x[1])
        )

        rejected = (
            parsed
            .filter(lambda x: not x[0])
            .map(lambda x: x[1])
        )

        with job_group(sc, "count_valid_rejected"):
            valid_count = valid.count()
            rejected_count = rejected.count()

        if valid_count + rejected_count != total_count:
            raise ValueError("Log reconciliation failed")

        pairs = valid.map(
            lambda row: (row[5], 1)
        )

        # Compare aggregation strategies
        if benchmark:

            report = benchmark_aggregation(pairs)

            metrics_dir = Path("artifacts/metrics")
            metrics_dir.mkdir(
                parents=True, exist_ok=True
            )

            metrics_path = (
                metrics_dir
                / f"rdd_aggregation_{raw_dir.name}.json"
            )

            metrics_path.write_text(
                json.dumps(report, indent=2),
                encoding="utf-8"
            )

        # Final aggregation
        with job_group(sc, "aggregate_country"):

            country_totals = pairs.reduceByKey(
                lambda a, b: a + b,
                numPartitions=4
            )

            summary = dict(country_totals.collect())

            top_10 = country_totals.takeOrdered(
                10,
                key=lambda x: (-x[1], x[0])
            )

        if sum(summary.values()) != valid_count:
            raise ValueError("Country summary mismatch")

        # Convert RDD to DataFrame
        valid_df = spark.createDataFrame(
            valid,
            schema=LOG_SCHEMA
        )

        rejected_df = spark.createDataFrame(
            rejected,
            schema=REJECT_SCHEMA
        )

        summary_df = spark.createDataFrame(
            list(summary.items()),
            schema=SUMMARY_SCHEMA
        )

        outputs = {
            "web_logs": (valid_df, valid_count),
            "web_logs_rejected": (
                rejected_df, rejected_count
            ),
            "web_log_country_summary": (
                summary_df, len(summary)
            )
        }

        mode = "overwrite" if overwrite else "error"

        for name, (df, expected) in outputs.items():

            path = str(output_dir / name)

            with job_group(sc, f"write_{name}"):

                df.write.mode(mode).parquet(path)

            with job_group(sc, f"validate_{name}"):

                actual = spark.read.parquet(path).count()

            if actual != expected:
                raise ValueError(
                    f"{name}: expected {expected}, "
                    f"actual {actual}"
                )

        result = {
            "total": total_count,
            "valid": valid_count,
            "rejected": rejected_count,
            "country_summary": summary
        }

        print("\nWEB LOG PIPELINE COMPLETED")
        print(json.dumps(result, indent=2))

        print(
            "\nAccumulator monitoring:",
            monitored_invalid
        )

        print("Top 10 countries:", top_10)

        print("\nRDD LINEAGE")
        print(
            country_totals.toDebugString()
            .decode("utf-8")
        )

        return result

    finally:

        parsed.unpersist(blocking=True)

        country_bc.destroy()


# =========================================
# 7. CLI
# =========================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument(
        "--overwrite", action="store_true"
    )

    parser.add_argument(
        "--benchmark", action="store_true"
    )

    parser.add_argument(
        "--hold-ui", action="store_true"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Ecommerce-Web-Log-Processing")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Spark UI:", spark.sparkContext.uiWebUrl)

    try:

        run_web_logs(
            spark,
            args.input,
            args.output,
            args.overwrite,
            args.benchmark
        )

        if args.hold_ui:
            input("Press Enter to stop Spark...")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
