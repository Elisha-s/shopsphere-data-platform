from delta.tables import DeltaTable
"""
    Generic Delta MERGE utility.
"""

def merge_delta_table(spark, source_df, target_path, merge_condition):
    target = DeltaTable.forPath(spark,target_path)(target.alias("target").merge(source_df.alias("source"), merge_condition)
                                                   .whenMatchedUpdateAll()
                                                   .whenNotMatchedInsertAll()
                                                   .execute())