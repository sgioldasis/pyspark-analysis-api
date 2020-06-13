import pytest
from etl.batch import (
    get_spark,
    read_data,
    calculate_kpi1,
    calculate_kpi2,
    write_jdbc,
    run_pipeline
)
import config


def test_read_files():

    spark = get_spark()
    data = read_data(spark, config.INPUT_PATH)

    expected_count = 40
    assert data.count() == expected_count


def test_kpi1_five_minutes():

    spark = get_spark()
    data = read_data(spark, config.INPUT_PATH)
    actual_df = calculate_kpi1(data, "5 minutes", "5-minute")

    expected_count = 6
    assert actual_df.count() == expected_count

    expected_data = [
        (1488355200000, 1488355500000, 1, 16100, "5-minute"),
        (1488355200000, 1488355500000, 3, 11500, "5-minute"),
        (1488355200000, 1488355500000, 2, 9260, "5-minute"),
        (1488355500000, 1488355800000, 1, 16100, "5-minute"),
        (1488355500000, 1488355800000, 3, 11500, "5-minute"),
        (1488355500000, 1488355800000, 2, 9260, "5-minute")
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["interval_start_timestamp",
                        "interval_end_timestamp", "service_id", "total_bytes", "interval"]
    )
    assert expected_df.collect() == actual_df.collect()


def test_kpi1_one_hour():

    spark = get_spark()
    data = read_data(spark, config.INPUT_PATH)
    actual_df = calculate_kpi1(data, "1 hour", "1-hour")

    expected_count = 3
    assert actual_df.count() == expected_count

    expected_data = [
        (1488355200000, 1488358800000, 1, 32200, "1-hour"),
        (1488355200000, 1488358800000, 3, 23000, "1-hour"),
        (1488355200000, 1488358800000, 2, 18520, "1-hour")
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["interval_start_timestamp",
                        "interval_end_timestamp", "service_id", "total_bytes", "interval"]
    )
    assert expected_df.collect() == actual_df.collect()


def test_kpi2_five_minutes():

    spark = get_spark()
    data = read_data(spark, config.INPUT_PATH)
    actual_df = calculate_kpi2(data, "5 minutes", "5-minute")

    assert True

    expected_count = 6
    assert actual_df.count() == expected_count

    expected_data = [
        (1488355200000, 1488355500000, 1001, 4, "5-minute"),
        (1488355200000, 1488355500000, 5005, 3, "5-minute"),
        (1488355200000, 1488355500000, 1000, 3, "5-minute"),
        (1488355500000, 1488355800000, 1001, 4, "5-minute"),
        (1488355500000, 1488355800000, 5005, 3, "5-minute"),
        (1488355500000, 1488355800000, 1000, 3, "5-minute")
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["interval_start_timestamp",
                        "interval_end_timestamp", "cell_id", "number_of_unique_users", "interval"]
    )
    assert expected_df.collect() == actual_df.collect()


def test_kpi2_one_hour():

    spark = get_spark()
    data = read_data(spark, config.INPUT_PATH)
    actual_df = calculate_kpi2(data, "1 hour", "1-hour")

    assert True

    expected_count = 3
    assert actual_df.count() == expected_count

    expected_data = [
        (1488355200000, 1488358800000, 1001, 4, "1-hour"),
        (1488355200000, 1488358800000, 5005, 3, "1-hour"),
        (1488355200000, 1488358800000, 1000, 3, "1-hour")
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["interval_start_timestamp",
                        "interval_end_timestamp", "cell_id", "number_of_unique_users", "interval"]
    )
    assert expected_df.collect() == actual_df.collect()


def test_run_pipeline():

    run_pipeline()

    # Get Spark
    spark = get_spark()

    # Check KPI1
    actual_df_kpi1 = spark.read.format("jdbc") \
        .option("url", config.JDBC_URL) \
        .option("dbtable", "kpi1") \
        .option("user", config.USER) \
        .option("password", config.PASSWORD) \
        .option("driver", config.JDBC_DRIVER) \
        .load()

    # actual_df_kpi1.show(n=10, truncate=False, vertical=False)

    expected_count_kpi1 = 9
    assert actual_df_kpi1.count() == expected_count_kpi1

    expected_data_kpi1 = [
        (1488355200000, 1488355500000, 1, 16100, "5-minute"),
        (1488355200000, 1488355500000, 3, 11500, "5-minute"),
        (1488355200000, 1488355500000, 2, 9260, "5-minute"),
        (1488355500000, 1488355800000, 1, 16100, "5-minute"),
        (1488355500000, 1488355800000, 3, 11500, "5-minute"),
        (1488355500000, 1488355800000, 2, 9260, "5-minute"),
        (1488355200000, 1488358800000, 1, 32200, "1-hour"),
        (1488355200000, 1488358800000, 3, 23000, "1-hour"),
        (1488355200000, 1488358800000, 2, 18520, "1-hour")
    ]
    expected_df_kpi1 = spark.createDataFrame(
        expected_data_kpi1, ["interval_start_timestamp",
                             "interval_end_timestamp", "service_id", "total_bytes", "interval"]
    )
    assert expected_df_kpi1.collect() == actual_df_kpi1.collect()

    # Check KPI2
    actual_df_kpi2 = spark.read.format("jdbc") \
        .option("url", config.JDBC_URL) \
        .option("dbtable", "kpi2") \
        .option("user", config.USER) \
        .option("password", config.PASSWORD) \
        .option("driver", config.JDBC_DRIVER) \
        .load()

    # actual_df_kpi2.show(n=10, truncate=False, vertical=False)

    expected_count_kpi2 = 9
    assert actual_df_kpi2.count() == expected_count_kpi2

    expected_data_kpi2 = [
        (1488355200000, 1488355500000, 1001, 4, "5-minute"),
        (1488355200000, 1488355500000, 5005, 3, "5-minute"),
        (1488355200000, 1488355500000, 1000, 3, "5-minute"),
        (1488355500000, 1488355800000, 1001, 4, "5-minute"),
        (1488355500000, 1488355800000, 5005, 3, "5-minute"),
        (1488355500000, 1488355800000, 1000, 3, "5-minute"),
        (1488355200000, 1488358800000, 1001, 4, "1-hour"),
        (1488355200000, 1488358800000, 5005, 3, "1-hour"),
        (1488355200000, 1488358800000, 1000, 3, "1-hour")
    ]
    expected_df_kpi2 = spark.createDataFrame(
        expected_data_kpi2, ["interval_start_timestamp",
                             "interval_end_timestamp", "cell_id", "number_of_unique_users", "interval"]
    )
    assert expected_df_kpi2.collect() == actual_df_kpi2.collect()
