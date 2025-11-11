from functools import reduce
from typing import List, Tuple
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import date_format, col


def add_columns(df: DataFrame, columns: List[Tuple[str, Column]]) -> DataFrame:
    """
    Add multiple columns to a DataFrame efficiently
    
    Args:
        df: Input DataFrame
        columns: List of (column_name, column_expression) tuples
    
    Returns:
        DataFrame with new columns added
    
    Example:
        >>> from pyspark.sql.functions import lit, col
        >>> columns = [
        ...     ("new_col1", lit(1)),
        ...     ("new_col2", col("old_col") * 2)
        ... ]
        >>> result_df = add_columns(df, columns)
    """
    return reduce(
        lambda acc_df, col_def: acc_df.withColumn(col_def[0], col_def[1]),
        columns,
        df
    )


def convert_timestamps_to_iso(df: DataFrame, timestamp_columns: List[str]) -> DataFrame:
    """
    Convert timestamp columns to ISO 8601 format for OpenSearch/Elasticsearch
    
    Args:
        df: Input DataFrame
        timestamp_columns: List of timestamp column names to convert
    
    Returns:
        DataFrame with timestamps converted to ISO 8601 strings
    
    Example:
        >>> ts_cols = ["created_at", "updated_at"]
        >>> result_df = convert_timestamps_to_iso(df, ts_cols)
    """
    return reduce(
        lambda acc_df, col_name: acc_df.withColumn(
            col_name,
            date_format(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ),
        timestamp_columns,
        df
    )
