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
    reduce_log
)


# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file1_path = "../data/consistency_1.csv"
file2_path = "../data/consistency_2.csv"


if __name__ == '__main__':

    # Load the CSV file into a PySpark DataFrame
    df1 = read_csv(spark, "csv", "true", ",", "true", file1_path)
    df2 = read_csv(spark, "csv", "true", ",", "true", file2_path)

    print(end="\n\n")
    print("Display the original DataFrame1 Columns")
    print(df1.columns)

    print(end="\n\n")
    print("Display the original DataFrame2 Columns")
    print(df2.columns)

    print(end="\n\n")
    print(
        "Ensure consistency in column names, rename columns [withColumnRenamed]")
    df1 = df1.withColumnRenamed("City_City", "City")
    df2 = (df2.withColumnRenamed("Age_Age", "Age")
              .withColumnRenamed("Occupation_Occupation", "Occupation"))

    print(end="\n\n")
    print("Display DataFrame1 Columns Consistency")
    print(df1.columns)

    print(end="\n\n")
    print("Display DataFrame2 Columns Consistency")
    print(df2.columns)
