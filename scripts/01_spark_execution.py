import argparse
import time
from statistics import median

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process_partition(rows):
    import time

    # Mô phỏng mỗi task mất khoảng 1 giây
    time.sleep(1)
    yield from rows


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--hold-ui", action="store_true")

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Phase01-OrderAnalytics")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    try:
        print("Spark version:", spark.version)
        print("Master:", sc.master)
        print("Parallelism:", sc.defaultParallelism)
        print("Spark UI:", sc.uiWebUrl)

        # 1. Read data
        orders = (
            spark.read
            .option("header", True)
            .schema("""
                order_id STRING,
                customer_id STRING,
                order_ts TIMESTAMP,
                status STRING
            """)
            .csv(args.input)
        )

        # 2. Transform data
        summary = (
            orders
            .filter(F.col("status") == "completed")
            .groupBy("customer_id")
            .agg(
                F.count("order_id").alias("total_orders")
            )
        )

        # 3. Execution plan
        summary.explain("formatted")

        # 4. Write output
        summary.write.mode("overwrite").parquet(
            args.output
        )

        # 5. Validate persisted output
        output = spark.read.parquet(args.output)

        output.show()

        assert output.count() == 10

        assert output.agg(
            F.sum("total_orders")
        ).first()[0] == 75

        print("Data validation passed!")

        # 6. Parallelism experiment
        rdd = sc.parallelize(range(8), 8)

        # Warm-up Python workers
        rdd.count()

        durations = []

        for run in range(3):
            sc.setJobGroup(
                f"parallelism_{run}",
                f"Parallelism experiment {run + 1}"
            )

            start = time.perf_counter()

            result = rdd.mapPartitions(
                process_partition
            ).collect()

            elapsed = time.perf_counter() - start
            durations.append(elapsed)

            assert sorted(result) == list(range(8))

            print(f"Run {run + 1}: {elapsed:.2f}s")

        print(
            f"Median runtime: {median(durations):.2f}s"
        )

        print("All tests passed!")

        if args.hold_ui:
            input("Press Enter to stop Spark...")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
