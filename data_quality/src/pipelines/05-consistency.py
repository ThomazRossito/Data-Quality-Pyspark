# you can ensure that your Spark dataframes or RDDs have a consistent structure and format,
# especially when you’re working with multiple data sources or transformations.

# Suppose you have two csv files representing information about employees,
# but the column names are slightly different. The goal is to load these CSV files
# into PySpark dataframes and ensure consistency in column names before
# further processing.

# Import Functions
from data_quality.src.functions.usefull import (
    sessionSpark,
    read_csv,
    reduce_log,
    rename_columns
)

from data_quality.src.functions.logger import logger

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file1_path = "../data/consistency_1.csv"
file2_path = "../data/consistency_2.csv"

def execute():
    # Load the CSV file into a PySpark DataFrame
    df1 = read_csv(spark, "csv", "true", ",", "true", file1_path)
    df2 = read_csv(spark, "csv", "true", ",", "true", file2_path)

    print(end="\n\n")
    logger.info("Display the original DataFrame1 Columns")
    logger.info(df1.columns)

    print(end="\n\n")
    logger.info("Display the original DataFrame2 Columns")
    logger.info(df2.columns)

    print(end="\n\n")
    logger.info("Ensure consistency in column names, rename columns [withColumnRenamed]")

    new_columns_1 = {"City_City": "City"}
    df1 = rename_columns(df1, new_columns_1)

    new_columns_2 = {"Age_Age": "Age", "Occupation_Occupation": "Occupation"}
    df2 = rename_columns(df2, new_columns_2)

    print(end="\n\n")
    logger.info("Display DataFrame1 Columns Consistency")
    logger.info(df1.columns)

    print(end="\n\n")
    logger.info("Display DataFrame2 Columns Consistency")
    logger.info(df2.columns)

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "consistency")

if __name__ == '__main__':
    execute()

