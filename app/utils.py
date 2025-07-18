from pyspark.sql import functions as F

def load_merchants(spark, path):
    """Load merchants CSV as Spark DataFrame."""
    return spark.read.csv(path, header=True, inferSchema=True)

def load_transactions(spark, path):
    """Load transactions Parquet as Spark DataFrame."""
    return spark.read.parquet(path)

def fill_missing_merchant_name(df, merchant_name_col='merchant_name', merchant_id_col='merchant_id', out_col='merchant'):
    """Fill missing merchant_name with merchant_id."""
    return df.withColumn(
        out_col,
        F.when(F.col(merchant_name_col).isNull(), F.col(merchant_id_col)).otherwise(F.col(merchant_name_col))
    )

def fill_missing_category(df, category_col='category', out_col='category'):
    """Fill missing category with 'Unknown category'."""
    return df.withColumn(
        out_col,
        F.when(F.col(category_col).isNull(), F.lit('Unknown category')).otherwise(F.col(category_col))
    )

def add_month_column(df, date_col='purchase_date', out_col='month'):
    """Add a month column in 'MMM yyyy' format from a date column."""
    return df.withColumn(out_col, F.date_format(date_col, 'MMM yyyy'))

def add_hour_column(df, date_col='purchase_date', out_col='hour'):
    """Add an hour column in 'HHmm' format from a date column."""
    return df.withColumn(out_col, F.date_format(date_col, 'HHmm'))

def drop_ambiguous_column(df, col):
    """Drop a column from a DataFrame if it exists."""
    if col in df.columns:
        return df.drop(col)
    return df
