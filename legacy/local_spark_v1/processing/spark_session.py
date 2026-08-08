from pyspark.sql import SparkSession

from configs.config import SPARK_APP_NAME, SPARK_MASTER, LOG_LEVEL


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)

        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")

        .config("spark.sql.shuffle.partitions", "4")

        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                "io.delta:delta-spark_2.12:3.2.0"
            ])
        )

        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )

        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(LOG_LEVEL)

    return spark









# from pyspark.sql import SparkSession

# from configs.config import SPARK_APP_NAME, SPARK_MASTER, LOG_LEVEL


# def create_spark_session() -> SparkSession:


#     spark = (
#         SparkSession.builder
#         .appName(SPARK_APP_NAME)
#         .config("spark.ui.enabled", "true")
#         .config("spark.ui.port", "4040")
#         .master(SPARK_MASTER)

#         .config(
#             "spark.jars.packages",
#             ",".join([
#                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
#                 "io.delta:delta-spark_2.12:3.2.0"
#             ])
#         )

#         .config(
#             "spark.sql.extensions",
#             "io.delta.sql.DeltaSparkSessionExtension"
#         )

#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog"
#         )
#         .config("spark.sql.adaptive.enabled", "true")
#         .config("spark.databricks.delta.optimizeWrite.enabled","true")
#         .config("spark.sql.shuffle.partitions", "4")
#         .config("spark.driver.memory", "2g")
#         .config("spark.databricks.delta.autoCompact.enabled","true")

#         .getOrCreate()
#     )

#     spark.sparkContext.setLogLevel(LOG_LEVEL)

#     return spark











# # from pyspark.sql import SparkSession

# # from configs.config import SPARK_APP_NAME, SPARK_MASTER, LOG_LEVEL


# # def create_spark_session() -> SparkSession:

# #     spark = (
# #         SparkSession.builder
# #         .appName(SPARK_APP_NAME)
# #         .master(SPARK_MASTER)
# #         .config(
# #             "spark.jars.packages",
# #             ",".join([
# #                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
# #                 "io.delta:delta-spark_2.12:3.2.0"
# #             ])
# #         )
# #         .config(
# #             "spark.sql.extensions",
# #             "io.delta.sql.DeltaSparkSessionExtension"
# #         )
# #         .config(
# #             "spark.sql.catalog.spark_catalog",
# #             "org.apache.spark.sql.delta.catalog.DeltaCatalog"
# #         )
# #         .getOrCreate()
# #     )

# #     spark.sparkContext.setLogLevel(LOG_LEVEL)

# #     return spark