from pyspark.sql import DataFrame

def write_gold(df : DataFrame, output_path:str):
    """
    Write a gold dataframe to delta lake.

    Args:
        df (DataFrame): The DataFrame to be written.
        output_path (str): The path where the DataFrame will be written.
    """

    return(
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    