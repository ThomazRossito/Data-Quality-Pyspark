# Suppose you have a csv file containing
# information about employees with duplicate values.

# Import Functions
from data_quality.src.functions.usefull import (
    sessionSpark,
    read_csv,
    reduce_log
)

from pyspark.sql.functions import (
    col,
    sum
)

from data_quality.src.functions.logger import logger

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file_path = "../data/completeness.csv"

def execute():
    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    logger.info("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    logger.info("Check completeness: Count null values in each column")
    df_count_null = df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns))

    print(end="\n\n")
    logger.info("Display count null DataFrame")
    df_count_null.show(truncate=False)

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "completeness")

if __name__ == '__main__':
    execute()