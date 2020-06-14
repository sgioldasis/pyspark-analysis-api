import pytest
from etl.batch import (
    read_csv,
    calculate_kpi1,
    calculate_kpi2,
    write_jdbc,
    run_pipeline
)
from etl.spark import get_spark
import config
from collections import Counter

KPI1_COLUMNS = [
    "interval_start_timestamp",
    "interval_end_timestamp",
    "service_id",
    "total_bytes",
    "interval"
]

KPI2_COLUMNS = [
    "interval_start_timestamp",
    "interval_end_timestamp",
    "cell_id",
    "number_of_unique_users",
    "interval"
]

DURATION_5_MINS = "5 minutes"
TAG_5_MINS = "5-minute"

DURATION_1_HOUR = "1 hour"
TAG_1_HOUR = "1-hour"

INTERVAL_5_MINS = (DURATION_5_MINS, TAG_5_MINS)
INTERVAL_1_HOUR = (DURATION_1_HOUR, TAG_1_HOUR)


def same(s, t):
    return Counter(s) == Counter(t)


def check(component, interval_duration, interval_tag, expected_data, expected_columns):
    data = read_csv(config.INPUT_PATH)
    actual_df = component(data, interval_duration, interval_tag)
    expected_df = get_spark().createDataFrame(expected_data, expected_columns)

    assert same(actual_df.columns, expected_columns)
    assert actual_df.count() == len(expected_data)
    assert expected_df.collect() == actual_df.collect()


def db_read_table(table_name):
    return (
        get_spark().read
        .format("jdbc")
        .option("url", config.JDBC_URL)
        .option("dbtable", table_name)
        .option("user", config.USER)
        .option("password", config.PASSWORD)
        .option("driver", config.JDBC_DRIVER)
        .load()
    )


def test_read_files():

    actual_df = read_csv(config.INPUT_PATH)
    assert actual_df.count() == 40


def test_kpi1_five_minutes():

    component = calculate_kpi1
    interval_duration, interval_tag = INTERVAL_5_MINS
    expected_columns = KPI1_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1, 16100, TAG_5_MINS),
        (1488355200000, 1488355500000, 3, 11500, TAG_5_MINS),
        (1488355200000, 1488355500000, 2, 9260, TAG_5_MINS),
        (1488355500000, 1488355800000, 1, 16100, TAG_5_MINS),
        (1488355500000, 1488355800000, 3, 11500, TAG_5_MINS),
        (1488355500000, 1488355800000, 2, 9260, TAG_5_MINS)
    ]

    check(component, interval_duration, interval_tag,
          expected_data, expected_columns)


def test_kpi1_one_hour():

    component = calculate_kpi1
    interval_duration, interval_tag = INTERVAL_1_HOUR
    expected_columns = KPI1_COLUMNS
    expected_data = [
        (1488355200000, 1488358800000, 1, 32200, TAG_1_HOUR),
        (1488355200000, 1488358800000, 3, 23000, TAG_1_HOUR),
        (1488355200000, 1488358800000, 2, 18520, TAG_1_HOUR)
    ]

    check(component, interval_duration, interval_tag,
          expected_data, expected_columns)


def test_kpi2_five_minutes():

    component = calculate_kpi2
    interval_duration, interval_tag = INTERVAL_5_MINS
    expected_columns = KPI2_COLUMNS

    expected_data = [
        (1488355200000, 1488355500000, 1001, 4, TAG_5_MINS),
        (1488355200000, 1488355500000, 5005, 3, TAG_5_MINS),
        (1488355200000, 1488355500000, 1000, 3, TAG_5_MINS),
        (1488355500000, 1488355800000, 1001, 4, TAG_5_MINS),
        (1488355500000, 1488355800000, 5005, 3, TAG_5_MINS),
        (1488355500000, 1488355800000, 1000, 3, TAG_5_MINS)
    ]

    check(component, interval_duration, interval_tag,
          expected_data, expected_columns)


def test_kpi2_one_hour():

    component = calculate_kpi2
    interval_duration, interval_tag = INTERVAL_1_HOUR
    expected_columns = KPI2_COLUMNS

    expected_data = [
        (1488355200000, 1488358800000, 1001, 4, TAG_1_HOUR),
        (1488355200000, 1488358800000, 5005, 3, TAG_1_HOUR),
        (1488355200000, 1488358800000, 1000, 3, TAG_1_HOUR)
    ]

    check(component, interval_duration, interval_tag,
          expected_data, expected_columns)


def test_run_pipeline():

    # Run the pipeline
    run_pipeline()

    # Check KPI1
    expected_table_name = "kpi1"
    expected_columns = KPI1_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1, 16100, TAG_5_MINS),
        (1488355200000, 1488355500000, 3, 11500, TAG_5_MINS),
        (1488355200000, 1488355500000, 2, 9260, TAG_5_MINS),
        (1488355500000, 1488355800000, 1, 16100, TAG_5_MINS),
        (1488355500000, 1488355800000, 3, 11500, TAG_5_MINS),
        (1488355500000, 1488355800000, 2, 9260, TAG_5_MINS),
        (1488355200000, 1488358800000, 1, 32200, TAG_1_HOUR),
        (1488355200000, 1488358800000, 3, 23000, TAG_1_HOUR),
        (1488355200000, 1488358800000, 2, 18520, TAG_1_HOUR)
    ]
    expected_df = get_spark().createDataFrame(expected_data, expected_columns)

    # Get actual data from database
    actual_df = db_read_table(expected_table_name)

    # Assertions
    assert same(actual_df.columns, expected_columns)
    assert actual_df.count() == len(expected_data)
    assert actual_df.collect() == expected_df.collect()

    # Check KPI2
    expected_table_name = "kpi2"
    expected_columns = KPI2_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1001, 4, TAG_5_MINS),
        (1488355200000, 1488355500000, 5005, 3, TAG_5_MINS),
        (1488355200000, 1488355500000, 1000, 3, TAG_5_MINS),
        (1488355500000, 1488355800000, 1001, 4, TAG_5_MINS),
        (1488355500000, 1488355800000, 5005, 3, TAG_5_MINS),
        (1488355500000, 1488355800000, 1000, 3, TAG_5_MINS),
        (1488355200000, 1488358800000, 1001, 4, TAG_1_HOUR),
        (1488355200000, 1488358800000, 5005, 3, TAG_1_HOUR),
        (1488355200000, 1488358800000, 1000, 3, TAG_1_HOUR)
    ]
    expected_df = get_spark().createDataFrame(expected_data, expected_columns)

    # Get actual data from database
    actual_df = db_read_table(expected_table_name)

    # Assertions
    assert same(actual_df.columns, expected_columns)
    assert actual_df.count() == len(expected_data)
    assert actual_df.collect() == expected_df.collect()
