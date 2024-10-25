# Suppose that your csv file contain information about employees
# and we want to ensure the accuracy of the age information by
# identifying and handling potential errors.

# Import Functions
from data_quality.src.functions.usefull import (
    sessionSpark,
    read_csv,
    reduce_log
)

from pyspark.sql.functions import (
    col,
    when
)

from data_quality.src.functions.logger import logger

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file_path = "../data/accuracy.csv"

def execute():
    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    logger.info("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    logger.info("Accuracy check: Identify and handle errors in the 'Age' column")
    df_cleaned = (
        df.withColumn("Age", when((col("Age").cast("int").isNull())
                                      | (col("Age") <= 0)
                                      | (col("Age") >= 110), None)
                                     .otherwise(col("Age"))))

    print(end="\n\n")
    logger.info("Display the cleaned DataFrame")
    df_cleaned.show(truncate=False)

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "accuracy")

if __name__ == '__main__':
    execute()