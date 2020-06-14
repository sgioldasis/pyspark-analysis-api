from pyspark.sql import SparkSession
from functools import lru_cache
import config


@lru_cache(maxsize=None)
def get_spark():
    spark = (
        SparkSession.builder
        .master("local")
        .appName("etl")
        .config("spark.driver.extraClassPath", config.JDBC_JAR)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark
