try:
    from pyspark.sql.types import StringType
except ImportError:
    StringType = None
from pyspark.sql import functions as F

def trim_and_standardize(df, columns=None, case='lower'):
    """Trim whitespace and standardize case for specified string columns in a single pass."""
    if columns is None:
        if StringType:
            columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        else:
            columns = [f.name for f in df.schema.fields if f.dataType.simpleString() == 'string']
    exprs = []
    for col in df.columns:
        if col in columns:
            col_expr = F.trim(F.col(col))
            if case == 'lower':
                col_expr = F.lower(col_expr)
            elif case == 'upper':
                col_expr = F.upper(col_expr)
            exprs.append(col_expr.alias(col))
        else:
            exprs.append(F.col(col))
    return df.select(*exprs)

def remove_duplicates(df, subset=None):
    """Remove duplicate rows. Optionally specify subset of columns (e.g., primary keys)."""
    return df.dropDuplicates(subset)

def handle_missing_values(df, fill_dict=None):
    """Fill missing values using a dictionary of column: value pairs."""
    if fill_dict is None:
        fill_dict = {}
    return df.fillna(fill_dict)

def convert_types(df, type_dict):
    """Convert columns to specified types. type_dict: {col: spark_type}"""
    for col, dtype in type_dict.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df

def remove_outliers(df, col, lower=None, upper=None):
    """Remove rows where col is outside [lower, upper]."""
    if lower is not None:
        df = df.filter(F.col(col) >= lower)
    if upper is not None:
        df = df.filter(F.col(col) <= upper)
    return df

def clean_dataframe(df, string_columns=None, case='lower', dedup_keys=None, fill_dict=None, cache=False):
    """Efficiently clean a DataFrame: trim, standardize case, deduplicate, fill missing, and optionally cache."""
    df = trim_and_standardize(df, columns=string_columns, case=case)
    if dedup_keys:
        df = remove_duplicates(df, subset=dedup_keys)
    if fill_dict:
        df = handle_missing_values(df, fill_dict=fill_dict)
    if cache:
        df = df.cache()
    return df
