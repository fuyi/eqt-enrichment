from unittest import TestCase
import pytest
from pyspark.sql import SparkSession

from . import schema_pf

spark = SparkSession.builder.getOrCreate()


class TestSchemaCompanyPortfolio(TestCase):
    def test_portfolio_schema_without_entry(self):
        df = (
            spark.read
                 .schema(schema_pf)
                 .option("mode", "FAILFAST")
                 .json('tests/fixture/portfolio_empty_entry.json')
        )
        with pytest.raises(Exception):
            df.show()
