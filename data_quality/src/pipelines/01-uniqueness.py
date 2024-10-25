# Suppose you have a csv file containing information about
# employees with duplicate values.

# Import Functions
from data_quality.src.functions.usefull import (
    sessionSpark,
    read_csv,
    reduce_log
)

from data_quality.src.functions.logger import logger

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file_path = "../data/exclusividade.csv"

def execute():
    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    logger.info("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    logger.info("display rows with duplicate values across all columns")
    df.exceptAll(df.dropDuplicates()).show(truncate=False)

    print(end="\n\n")
    logger.info("display rows with duplicate values across 'name' and 'age' columns")
    df.exceptAll(df.dropDuplicates(['name', 'age'])).show(truncate=False)

    print(end="\n\n")
    logger.info("display dataframe with values no duplicate")
    df.dropDuplicates().show(truncate=False)

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "uniqueness")


if __name__ == '__main__':
    execute()
