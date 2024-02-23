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

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file_path = "../data/accuracy.csv"


if __name__ == '__main__':

    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    print("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    print("Accuracy check: Identify and handle errors in the 'Age' column")
    df_cleaned = (df.withColumn("Age", when((col("Age").cast("int").isNull())
                                            | (col("Age") <= 0)
                                            | (col("Age") >= 110), None)
                                .otherwise(col("Age"))))

    print(end="\n\n")
    print("Display the cleaned DataFrame")
    df_cleaned.show(truncate=False)
