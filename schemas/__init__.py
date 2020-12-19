from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
    DecimalType,
    ArrayType
)

schema_funding_rounds = StructType([
    StructField('company_name', StringType(), False),
    StructField('announced_on', DateType(), True),
    StructField('investment_type', StringType(), True),
    StructField('investor_count', IntegerType(), True),
    StructField('investor_names', StringType(), True),
    StructField('raised_amount_usd', DecimalType(), True),
])

schema_org = StructType([
    StructField('company_name', StringType(), False),
    StructField('city', StringType(), True),
    StructField('country_code', StringType(), True),
    StructField('employee_count', StringType(), True),
    StructField('founded_on', DateType(), True),
    StructField('funding_rounds', StringType(), True),  # FIXME: IntegerType
    StructField('funding_total_usd', DecimalType(), True),
    StructField('homepage_url', StringType(), True),
    StructField('last_funding_on', DateType(), True),
    StructField('short_description', StringType(), True),
    StructField('description', StringType(), True)
])


# Define unified schema for portfolio company and dinvestment
schema_pf = StructType([
    StructField('SDG', ArrayType(StringType(), True), True),
    StructField('company_name', StringType(), False),
    StructField('sector', StringType(), False),
    StructField('country', StringType(), False),
    StructField('fund', StringType(), False),
    StructField('entry', DateType(), False),
    StructField('exit', DateType(), True),
])

schema_active_funds = StructType([
    StructField('fund', StringType(), False),
    StructField('size', StringType(), False),
    StructField('status', StringType(), False),
    StructField('launch_year', DateType(), False)
])
