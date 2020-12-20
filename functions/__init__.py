from pyspark.sql.dataframe import DataFrame


def dataframe_union(df1: DataFrame, df2: DataFrame) -> DataFrame:
    return df1.union(df2)
