from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

def flatten_df(df):
    """
    Recursively flattens all nested StructType and ArrayType fields in a PySpark DataFrame.
    
    Parameters:
        df (DataFrame): Input nested PySpark DataFrame

    Returns:
        DataFrame: Flattened PySpark DataFrame with all levels exploded and flattened
    """

    while True:
        # Detect nested Struct or Array fields
        complex_fields = [
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, (StructType, ArrayType))
        ]

        # Exit loop when no nested fields remain
        if not complex_fields:
            break

        col_name, dtype = complex_fields[0]

        # Flatten StructType by expanding its fields into individual columns
        if isinstance(dtype, StructType):
            expanded = [
                col(f"{col_name}.{nested.name}").alias(f"{col_name}_{nested.name}")
                for nested in dtype.fields
            ]
            df = df.select("*", *expanded).drop(col_name)

        # Explode ArrayType into multiple rows
        elif isinstance(dtype, ArrayType):
            df = df.withColumn(col_name, explode_outer(col(col_name)))

    return df
