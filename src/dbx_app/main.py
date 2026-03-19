import argparse
from datetime import datetime

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    table_name = f"{args.catalog}.{args.schema}.demo_app_runs"

    data = [
        ("dbx-app", args.catalog, args.schema, datetime.utcnow().isoformat(), "ok"),
    ]

    df = spark.createDataFrame(
        data,
        ["app_name", "catalog", "schema", "run_ts_utc", "status"],
    )

    print(f"A correr app com catalog={args.catalog} e schema={args.schema}")
    print(f"A gravar tabela: {table_name}")

    df.show(truncate=False)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )

    print(f"Tabela escrita com sucesso: {table_name}")


if __name__ == "__main__":
    main()