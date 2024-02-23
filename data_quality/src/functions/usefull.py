from pyspark.sql import SparkSession
from delta import DeltaTable
from pyspark.sql import DataFrame

## Spark Session
def sessionSpark(appName: str):
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName(appName)
            ## Delta Lake
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            ## Hive SQL
            .enableHiveSupport()
            .getOrCreate()
    )
    return spark


## Read CSV
def read_csv(spark: SparkSession, format: str, header: str, sep: str, inferSchema: str, path: str) -> DataFrame:
    return (spark.read
                .format(format)
                .option("header", header)
                .option("sep", sep)
                .option("inferSchema", inferSchema)
                .load(path))

# Reduce logging
def reduce_log(spark: SparkSession):
    return spark.sparkContext.setLogLevel("WARN")