from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Test")
    .getOrCreate()
)

print("Spark:", spark.version)
print("Java :", spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))

spark.stop()