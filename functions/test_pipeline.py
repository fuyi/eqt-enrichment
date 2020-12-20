from unittest import TestCase
from pyspark.sql import SparkSession

from . import dataframe_union

spark = SparkSession.builder.getOrCreate()


class TestPipline(TestCase):
    def test_portfolio_schema_without_entry(self):
        df1 = (
            spark.read
                 .json('tests/fixture/portfolio_empty_entry.json')
        )
        df_union = dataframe_union(df1, df1)
        assert df_union.count() == 2
