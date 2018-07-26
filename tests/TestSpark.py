from unittest import TestCase

import jaydebeapi
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf


def query_to_pandas():
    driver_args = ["SA", "CMP@ssword"]
    connection = jaydebeapi.connect(jclassname="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                                    url="jdbc:sqlserver://10.211.190.22;databaseName=DV6V_CM",
                                    driver_args=driver_args,
                                    jars=["drivers/mssql-jdbc-6.4.0.jre8.jar"])

    curs = connection.cursor()
    curs.execute("SELECT getdate()")
    result_set = curs.fetchall()
    df = pd.DataFrame(result_set, columns=["DATE"])
    return df


def query_with_spark():
    import os
    os.environ["HADOOP_HOME"] = f"{os.path.dirname(os.path.realpath(__file__))}/../drivers"

    conf = SparkConf()  # create the configuration
    conf.set("spark.jars", "drivers/mssql-jdbc-6.4.0.jre8.jar")  # set JDBC driver

    spark = SparkSession.builder \
        .master("local[2]") \
        .config(conf=conf) \
        .appName("testExtract") \
        .getOrCreate()

    try:
        jdbcDF = spark.read \
            .format("jdbc") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", "jdbc:sqlserver://10.211.190.22;databaseName=DV6V_CM") \
            .option("dbtable", "(SELECT getdate() as date) as test_table") \
            .option("user", "SA") \
            .option("password", "CMP@ssword") \
            .options(treatEmptyValuesAsNulls='true') \
            .load()
    except Exception:
        return None
    else:
        return jdbcDF


class TestJDBC(TestCase):
    def test_jdbc_with_pandas(self):
        df = query_to_pandas()
        print(df.shape)
        assert(df.shape == (1, 1))

    def test_jdbc_with_spark(self):
        df = query_with_spark()
        print(df)
        df.write.save("data/test.parquet")