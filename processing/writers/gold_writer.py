from pyspark.sql import DataFrame
from processing.utils.delta_merge import merge_delta_table
from delta.tables import DeltaTable
from typing import List


def write_gold(spark,df,key_columns: List[str], target_path:str):


    """
    Writes Gold tables.

    First run:
        Creates the Delta table.

    Subsequent runs:
        MERGEs into the existing table.
    """

    merge_condition = " AND ".join([
        f"target.{column} = source.{column}"
        for column in key_columns
    ])

    if not DeltaTable.isDeltaTable(
        spark,target_path
    ) :
        (df.write
         .format("delta")
         .mode("overwrite")
         .save(target_path)
         )
        
    else :

        merge_delta_table(spark = spark, source_df=df, target_path=target_path, merge_condition=merge_condition)



# def write_gold(df : DataFrame, output_path:str):
#     """
#     Write a gold dataframe to delta lake.

#     Args:
#         df (DataFrame): The DataFrame to be written.
#         output_path (str): The path where the DataFrame will be written.
#     """

#     return(
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .save(output_path)
#     )
    