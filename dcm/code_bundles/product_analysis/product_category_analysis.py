"""product_category_analysis.py - type:spark Code Bundle (pure PySpark).

Reads the raw orders/products Iceberg tables from the shared Glue lake (through
the catalog-linked database), computes category/subcategory performance with
rankings, and recreates PRODUCT_CATEGORY_ANALYSIS as a Snowflake-managed Iceberg
v3 table on every run.

Run by the pipeline task T_RUN_SPARK:
  EXECUTE CODE BUNDLE <db>.DATA_ENG_DEMO.CB_PRODUCT_ANALYSIS
    ENTRYPOINT = 'product_category_analysis.py'
    ARGUMENTS = ('--target-db', '<db>', '--lake-db', 'TLV_HOL_LAKE', '--lake-schema', 'tlv_hol_raw');
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from tabulate import tabulate


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-db", required=True, help="environment database, e.g. TLV_BUILD_HOL_DEV")
    ap.add_argument("--lake-db", default="TLV_HOL_LAKE")
    ap.add_argument("--lake-schema", default="tlv_hol_raw")
    args, _ = ap.parse_known_args()

    target = f"{args.target_db}.DATA_ENG_DEMO.PRODUCT_CATEGORY_ANALYSIS"
    lake = f"{args.lake_db}.{args.lake_schema}"

    spark = SparkSession.builder.getOrCreate()
    # Glue identifiers are lowercase. With the default caseSensitive=false,
    # Snowpark Connect upper-cases and quotes names (TLV_HOL_RAW.ORDERS), which
    # a Glue CLD can't resolve. Case-sensitive mode keeps names exactly as typed.
    spark.conf.set("spark.sql.caseSensitive", "true")

    orders = spark.table(f"{lake}.orders")
    products = spark.table(f"{lake}.products")

    category_performance = (
        orders.filter(F.col("status").isin(["COMPLETED", "SHIPPED"]))
        .join(products, "product_id")
        .groupBy("category", "subcategory")
        .agg(
            F.count("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("units_sold"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_revenue"),
            F.sum(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100)).alias("net_revenue"),
            F.sum(F.col("quantity") * F.col("cost_price")).alias("total_cost"),
        )
    )

    by_revenue = Window.orderBy(F.desc("net_revenue"))
    within_category = Window.partitionBy("category").orderBy(F.desc("net_revenue"))

    analysis = (
        category_performance
        .withColumn("gross_profit", F.col("net_revenue") - F.col("total_cost"))
        .withColumn("profit_margin_pct", F.round(F.col("gross_profit") / F.col("net_revenue") * 100, 2))
        .withColumn("revenue_per_customer", F.round(F.col("net_revenue") / F.col("unique_customers"), 2))
        .withColumn("overall_revenue_rank", F.dense_rank().over(by_revenue))
        .withColumn("category_revenue_rank", F.dense_rank().over(within_category))
        .select(
            "category",
            "subcategory",
            "order_count",
            "unique_customers",
            "units_sold",
            F.round("gross_revenue", 2).alias("gross_revenue"),
            F.round("net_revenue", 2).alias("net_revenue"),
            F.round("total_cost", 2).alias("total_cost"),
            F.round("gross_profit", 2).alias("gross_profit"),
            "profit_margin_pct",
            "revenue_per_customer",
            "overall_revenue_rank",
            "category_revenue_rank",
            F.current_timestamp().cast("timestamp_ntz").alias("analysis_timestamp"),
        )
    )

    # caseSensitive is on, so name the output columns in UPPERCASE: that keeps them
    # unquoted-friendly for SQL and dbt consumers.
    out = analysis.select([F.col(c).alias(c.upper()) for c in analysis.columns])

    # Recreate the target on every run as a Snowflake-managed Iceberg v3 table.
    # snowpark.connect.iceberg.external_volume selects the storage; format-version
    # is forwarded to Snowflake unchanged.
    spark.conf.set("snowpark.connect.iceberg.external_volume", "SNOWFLAKE_MANAGED")
    (
        out.writeTo(target)
        .using("iceberg")
        .tableProperty("format-version", "3")
        .tableProperty("comment", "TLV DE HOL Spark bundle output (Iceberg v3, managed storage); recreated by CB_PRODUCT_ANALYSIS each run.")
        .createOrReplace()
    )

    summary = (
        analysis.groupBy("category")
        .agg(F.sum("net_revenue").alias("net_revenue"), F.sum("gross_profit").alias("gross_profit"))
        .orderBy(F.desc("net_revenue"))
        .collect()
    )
    print(f"Wrote {analysis.count()} rows to {target}")
    print(tabulate([r.asDict() for r in summary], headers="keys", tablefmt="github", floatfmt=",.2f"))


if __name__ == "__main__":
    main()
