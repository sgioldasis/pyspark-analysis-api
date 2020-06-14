"""
This module provides the ETL batch pipeline and its individual components
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.window import Window
import config
from etl.spark import get_spark


def run_pipeline():
    """
    Runs all components of the ETL pipeline in the correct order
    """
    data = read_csv(config.INPUT_PATH)
    write_jdbc(
        df=calculate_kpi1(data, *config.INTERVAL_5_MINS), db_table="kpi1", mode="overwrite")
    write_jdbc(
        df=calculate_kpi1(data, *config.INTERVAL_1_HOUR), db_table="kpi1", mode="append")
    write_jdbc(
        df=calculate_kpi2(data, *config.INTERVAL_5_MINS), db_table="kpi2", mode="overwrite")
    write_jdbc(
        df=calculate_kpi2(data, *config.INTERVAL_1_HOUR), db_table="kpi2", mode="append")


def read_csv(folder_path):
    """
    Reads csv files and returns a dataframe.

    Reads all the csv files in the provided path into a dataframe using a
    specific schema. It then extends the dataframe with the following columns:

    "interval_start": Convert interval_start_timestamp to timestamp
    "interval_end"  : Convert interval_end_timestamp to timestamp
    "total_bytes"   : bytes_downlink + bytes_uplink

    Parameters
    ----------
    folder_path : str
        The filesystem path of the csv files

    Returns
    -------
    dataframe
        A dataframe with the csv columns plus the additional columns
    """

    # Define expected schema
    schema = StructType(
        [
            StructField("interval_start_timestamp", LongType()),
            StructField("interval_end_timestamp", LongType()),
            StructField("msisdn", LongType()),
            StructField("bytes_uplink", LongType()),
            StructField("bytes_downlink", LongType()),
            StructField("service_id", IntegerType()),
            StructField("cell_id", LongType()),
        ]
    )

    # Read text files into dataframe
    df = (
        get_spark().read.format("com.databricks.spark.csv")
        .schema(schema)
        .option("header", "true")
        .load(folder_path)
    )

    # Add columns
    df_extended = (
        df
        .withColumn("interval_start", F.to_timestamp(df.interval_start_timestamp / 1000))
        .withColumn("interval_end", F.to_timestamp(df.interval_end_timestamp / 1000))
        .withColumn("total_bytes", df.bytes_downlink + df.bytes_uplink)
    )

    # Return
    return df_extended


def calculate_kpi1(df_input, interval_duration, interval_tag):
    """
    Calculates KPI1: Top 3 services by traffic volume

    The top 3 services (as identified by service_id) which generated the largest 
    traffic volume in terms of bytes (downlink_bytes + uplink_bytes) for the
    interval

    Parameters
    ----------
    df_input : dataframe
        A dataframe containing the raw data
    interval_duration : str
        A duration string (eg. "5 minutes") for the desired breakdown interval
    interval_tag : str
        A string which will be used for the "interval" column of the ouput

    Returns
    -------
    dataframe
        A dataframe with the top 3 services for each interval 

        Columns:
        "interval_start_timestamp"
        "interval_end_timestamp"
        "service_id"
        "total_bytes"
    """
    return (
        df_input
        .groupBy("service_id", F.window("interval_start", interval_duration))
        .sum("total_bytes")
        .withColumnRenamed("sum(total_bytes)", "total_bytes")
        .withColumn("rank", F.dense_rank().over(
            Window
            .partitionBy("window")
            .orderBy(F.desc("total_bytes"), F.desc("service_id"))
        ))
        .where("rank <= 3")
        .selectExpr(
            "cast(window.start as long)*1000 as interval_start_timestamp",
            "cast(window.end as long)*1000 as interval_end_timestamp",
            "service_id",
            "total_bytes",
            f"'{interval_tag}' as interval"
        )
    )


def calculate_kpi2(df_input, interval_duration, interval_tag):
    """
    Calculates KPI2: Top 3 cells by number of unique users

    The top 3 cells (as identified by cell_id) which served the highest number 
    of unique users (as identified by msisdn) for the interval

    Parameters
    ----------
    df_input : dataframe
        A dataframe containing the raw data
    interval_duration : str
        A duration string (eg. "5 minutes") for the desired breakdown interval
    interval_tag : str
        A string which will be used for the "interval" column of the ouput

    Returns
    -------
    dataframe
        A dataframe with the top 3 cells for each interval. 

        Columns:
        "interval_start_timestamp"
        "interval_end_timestamp"
        "cell_id"
        "number_of_unique_users"
    """
    return (
        df_input
        .groupBy("cell_id", F.window("interval_start", interval_duration))
        .agg(F.countDistinct("msisdn"))
        .withColumnRenamed("count(msisdn)", "number_of_unique_users")
        .withColumn("rank", F.dense_rank().over(
            Window
            .partitionBy("window")
            .orderBy(F.desc("number_of_unique_users"), F.desc("cell_id")))
        )
        .where("rank <= 3")
        .selectExpr(
            "cast(window.start as long)*1000 as interval_start_timestamp",
            "cast(window.end as long)*1000 as interval_end_timestamp",
            "cell_id",
            "number_of_unique_users",
            f"'{interval_tag}' as interval"
        )
    )


def write_jdbc(df,
               db_table,
               jdbc_url=config.JDBC_URL,
               user=config.USER,
               password=config.PASSWORD,
               mode="append"
               ):
    """
    Writes a dataframe to a JDBC database using the provided parameters
    """
    df.write.format("jdbc") \
        .mode(mode) \
        .option("url", jdbc_url) \
        .option("dbtable", db_table) \
        .option("user", user) \
        .option("password", password) \
        .save()
