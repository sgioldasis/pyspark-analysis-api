import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.window import Window
import config
from etl.spark import get_spark


def run_pipeline():
    data = read_csv(config.INPUT_PATH)
    write_jdbc(
        df=calculate_kpi1(data, "5 minutes", "5-minute"), db_table="kpi1", mode="overwrite")
    write_jdbc(
        df=calculate_kpi1(data, "1 hour", "1-hour"), db_table="kpi1", mode="append")
    write_jdbc(
        df=calculate_kpi2(data, "5 minutes", "5-minute"), db_table="kpi2", mode="overwrite")
    write_jdbc(
        df=calculate_kpi2(data, "1 hour", "1-hour"), db_table="kpi2", mode="append")


def read_csv(folder_path):
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

    # df.show(n=10, truncate=False, vertical=False)

    return df_extended


def calculate_kpi1(df_input, interval_duration, interval_tag):
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
    df.write.format("jdbc") \
        .mode(mode) \
        .option("url", jdbc_url) \
        .option("dbtable", db_table) \
        .option("user", user) \
        .option("password", password) \
        .save()
