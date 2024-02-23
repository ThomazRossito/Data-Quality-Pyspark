# Suppose that your csv file contain information about employees and
# we want to ensure the validity of the ‘Occupation’ column. We’ll set a rule
# that each employee’s occupation must be one of a predefined set of valid
# occupations.

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
file_path = "../data/validity.csv"


if __name__ == '__main__':

    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    print("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    print("We add the rule and ensure that values of “Occupation” are part of a predefined set of valid occupations.")

    print(end="\n\n")
    print("Validity check: Ensure 'Occupation' is one of the valid occupations")

    valid_occupations = [
        "Engineer",
        "Teacher",
        "Software Developer",
        "Accountant",
        "Marketing Manager"]
    df_valid = (
        df.withColumn(
            "Occupation",
            when(
                col("Occupation").isin(valid_occupations),
                col("Occupation")) .otherwise(None)))

    print(end="\n\n")
    print("Display valid occupations notNull")
    df_valid.filter(col("Occupation").isNotNull()).show(truncate=False)

    print(end="\n\n")
    print("Display valid occupations null")
    df_valid.filter(col("Occupation").isNull()).show(truncate=False)
