# you can examine timestamps and ensure that the data
# is up-to-date based on a specified criterion


# Import Functions
from data_quality.src.functions.usefull import (
    sessionSpark,
    read_csv,
    reduce_log
)

from pyspark.sql.functions import (
    col,
    current_date
)

# Import Spark Session
spark = sessionSpark("data-quality-checks")

# Reduce logging
reduce_log(spark)

# Define the path to the CSV file
file_path = "../data/timeliness.csv"


if __name__ == '__main__':

    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    print("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    print("Timeliness check: Filter events that occurred within the last 7 days")
    days_threshold = 7
    df_timely = df.filter(
        (current_date() - col("EventDate")).cast("int") <= days_threshold)

    print(end="\n\n")
    print("Display the DataFrame after handling timeliness issues")
    df_timely.show(truncate=False)
