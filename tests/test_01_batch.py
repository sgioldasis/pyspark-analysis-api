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


# O(n) list comparizon ignoring order
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
    interval = config.INTERVAL_5_MINS
    expected_columns = config.KPI1_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1, 16100, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 3, 11500, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 2, 9260, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 1, 16100, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 3, 11500, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 2, 9260, config.TAG_5_MINS)
    ]
    check(component, *interval, expected_data, expected_columns)


def test_kpi1_one_hour():

    component = calculate_kpi1
    interval = config.INTERVAL_1_HOUR
    expected_columns = config.KPI1_COLUMNS
    expected_data = [
        (1488355200000, 1488358800000, 1, 32200, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 3, 23000, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 2, 18520, config.TAG_1_HOUR)
    ]
    check(component, *interval, expected_data, expected_columns)


def test_kpi2_five_minutes():

    component = calculate_kpi2
    interval = config.INTERVAL_5_MINS
    expected_columns = config.KPI2_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1001, 4, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 5005, 3, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 1000, 3, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 1001, 4, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 5005, 3, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 1000, 3, config.TAG_5_MINS)
    ]
    check(component, *interval, expected_data, expected_columns)


def test_kpi2_one_hour():

    component = calculate_kpi2
    interval = config.INTERVAL_1_HOUR
    expected_columns = config.KPI2_COLUMNS
    expected_data = [
        (1488355200000, 1488358800000, 1001, 4, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 5005, 3, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 1000, 3, config.TAG_1_HOUR)
    ]
    check(component, *interval, expected_data, expected_columns)


def test_run_pipeline():

    # Run the pipeline
    run_pipeline()

    # Check KPI1
    expected_table_name = config.KPI1_TABLE_NAME
    expected_columns = config.KPI1_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1, 16100, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 3, 11500, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 2, 9260, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 1, 16100, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 3, 11500, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 2, 9260, config.TAG_5_MINS),
        (1488355200000, 1488358800000, 1, 32200, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 3, 23000, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 2, 18520, config.TAG_1_HOUR)
    ]
    expected_df = get_spark().createDataFrame(expected_data, expected_columns)

    # Get actual data from database
    actual_df = db_read_table(expected_table_name)

    # Assertions
    assert same(actual_df.columns, expected_columns)
    assert actual_df.count() == len(expected_data)
    assert actual_df.collect() == expected_df.collect()

    # Check KPI2
    expected_table_name = config.KPI2_TABLE_NAME
    expected_columns = config.KPI2_COLUMNS
    expected_data = [
        (1488355200000, 1488355500000, 1001, 4, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 5005, 3, config.TAG_5_MINS),
        (1488355200000, 1488355500000, 1000, 3, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 1001, 4, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 5005, 3, config.TAG_5_MINS),
        (1488355500000, 1488355800000, 1000, 3, config.TAG_5_MINS),
        (1488355200000, 1488358800000, 1001, 4, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 5005, 3, config.TAG_1_HOUR),
        (1488355200000, 1488358800000, 1000, 3, config.TAG_1_HOUR)
    ]
    expected_df = get_spark().createDataFrame(expected_data, expected_columns)

    # Get actual data from database
    actual_df = db_read_table(expected_table_name)

    # Assertions
    assert same(actual_df.columns, expected_columns)
    assert actual_df.count() == len(expected_data)
    assert actual_df.collect() == expected_df.collect()
