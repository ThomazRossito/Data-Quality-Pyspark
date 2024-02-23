# Exclusividade:
# Isso garante que cada registro ou entidade de dados seja distinto
# e não tenha duplicatas no conjunto de dados.


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
file_path = "../data/exclusividade.csv"


if __name__ == '__main__':

    # Load the CSV file into a PySpark DataFrame
    df = read_csv(spark, "csv", "true", ",", "true", file_path)

    print(end="\n\n")
    print("Display the original DataFrame")
    df.show(truncate=False)

    print(end="\n\n")
    print("display rows with duplicate values across all columns")
    df.exceptAll(df.dropDuplicates()).show(truncate=False)

    print(end="\n\n")
    print("display rows with duplicate values across 'name' and 'age' columns")
    df.exceptAll(df.dropDuplicates(['name', 'age'])).show(truncate=False)
