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

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file_path = "../data/completeness.csv"


if __name__ == '__main__':

    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    print("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    print("Check completeness: Count null values in each column")
    df_count_null = df.select(
        *(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns))

    print(end="\n\n")
    print("Display count null DataFrame")
    df_count_null.show(truncate=0)
