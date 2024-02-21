from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Pyspark")

    spark = SparkSession \
        .builder \
        .appName("operation") \
        .master("local") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.csv("hdfs:///user/maria_dev/mongodb/train.csv", header=True, inferSchema=True)
    print("csv loaded")
    # Displaying the schema of the DataFrame
    df.printSchema()

    # Displaying the first few rows of the DataFrame
    print("Showing first few rows")
    df.show()

    spark.stop()
