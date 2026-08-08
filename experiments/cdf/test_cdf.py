from processing.spark_session import create_spark_session
from processing.readers.cdf_reader import read_changes

from pyspark.sql.functions import col

spark = create_spark_session()

cdf = read_changes(spark, None)

cdf = cdf.filter(
    col("_change_type") == "insert"
)

cdf.select(
    "_change_type",
    "_commit_version",
    "order_id",
    "amount"
).show(20, truncate=False)

spark.stop()



# from processing.spark_session import create_spark_session
# from processing.readers.cdf_reader import read_changes

# spark = create_spark_session()

# cdf = read_changes(spark, None)

# cdf.printSchema()

# spark.stop()