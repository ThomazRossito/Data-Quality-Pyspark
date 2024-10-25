# Exclusividade:
# Isso garante que cada registro ou entidade de dados seja distinto
# e não tenha duplicatas no conjunto de dados.

##### Executar com
##### spark-submit
"""
spark-submit --master local \
--packages io.delta:delta-spark_2.12:3.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
/Users/thomaz_rossito/Projects/data_quality_spark/data_quality/src/pipelines/01-uniqueness.py
"""

# Import Functions
from pyspark.sql import SparkSession
from delta import DeltaTable
from pyspark.sql import DataFrame

## Spark Session

spark = (
SparkSession.builder
    .master("local[*]")
    ## Delta Lake
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ## Hive SQL
    .enableHiveSupport()
    .getOrCreate()
)


spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.conf.set("spark.databricks.delta.vacuum.logging.enabled", True)


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
    return spark.sparkContext.setLogLevel("ERROR")

# Import Spark Session

# Reduce logging
reduce_log(spark)


# Define the path to the CSV file
file_path = "/Users/thomaz_rossito/Projects/data_quality_spark/data_quality/src/data/exclusividade.csv"

path_destination = "/Users/thomaz_rossito/Projects/data_quality_spark/data_quality/src/delta_table/exclusividade/"

def execute():
    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "false", file_path)

    print(end="\n\n")
    print("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    print("WRITE")
    df.write.format("delta").mode("overwrite").save(path_destination)

    deltaTable = DeltaTable.forPath(spark, path_destination)

    print(end="\n\n")
    print("OPTIMIZE")
    print(deltaTable.optimize().executeCompaction())

    print(end="\n\n")
    print("VACUUM")
    print(deltaTable.vacuum(0))

    print(end="\n\n")
    print("HISTORY")
    print(deltaTable.history().show(truncate=False))

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "uniqueness")

if __name__ == '__main__':
    execute()
