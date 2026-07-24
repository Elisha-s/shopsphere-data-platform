from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, count, avg, countDistinct

def calculate_total_revenue(df : DataFrame) -> DataFrame:
    """
    Calculate the total revenue from the orders DataFrame.

    Args:
        df (DataFrame): Silver DataFrame containing order data.

    Returns:
        DataFrame: A DataFrame with the total revenue metric.
    """
    return df.agg(
        sum("amount").alias("total_revenue")
    )

def calculate_customer_revenue(df : DataFrame) -> DataFrame:
    """
    Calculate the total revenue per customer from the orders DataFrame.

    Args:
        df (DataFrame): Silver DataFrame containing order data.

    Returns:
        DataFrame: A DataFrame with the total revenue per customer metric.
    """
    return df.groupBy("customer_id").agg(
        sum("amount").alias("total_revenue"), 
        count("*").alias("total_orders")
    )

def calculate_product_revenue(df : DataFrame) -> DataFrame:
    """
    Calculate the total revenue per product from the orders DataFrame.

    Args:
        df (DataFrame): Silver DataFrame containing order data.

    Returns:
        DataFrame: A DataFrame with the total revenue per product metric.
    """
    return df.groupBy("product_id").agg(
        sum("amount").alias("total_revenue"), 
        count("*").alias("total_orders")
    )

def build_sales_summary(df : DataFrame) -> DataFrame:
    return (
        df.agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("total_orders"),
            avg("amount").alias("average_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products")
        )
    )

def build_customer_metrics(df : DataFrame) -> DataFrame:
    return(
        df.groupBy("customer_id").agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("total_orders"),
            avg("amount").alias("average_order_value"),
        )
    )

def build_product_metrics(df : DataFrame) -> DataFrame:
    return(
        df.groupBy("product_id").agg(
            count("*").alias("total_orders"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("average_order_value")
        )
    )