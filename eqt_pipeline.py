from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, collect_list

import click
import logging
import os

from scrapy.crawler import CrawlerProcess

from eqt_crawler.eqt_crawler.spiders.eqt import EqtSpider
from schemas import (
    schema_funding_rounds,
    schema_org,
    schema_pf,
    schema_active_funds
)
from functions import dataframe_union

COMPANY_DETAIL_COLUMNS = [
    'sector',
    'country',
    'country_code',
    'city',
    'entry',
    'exit',
    'SDG',
    'description',
    'employee_count',
    'founded_on',
    'funding_rounds',
    'funding_total_usd',
    'homepage_url',
    'last_funding_on',
    'short_description'
]

FUND_DETAIL_COLUMNS = [
    'fund',
    'launch_year',
    'size',
    'status'
]

FUNDING_COLUMNS = [
    'announced_on',
    'investment_type',
    'investor_count',
    'investor_names',
    'raised_amount_usd',
]

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def run_crawler():
    c = CrawlerProcess({'USER_AGENT': 'Mozilla/5.0'})
    c.crawl(EqtSpider)
    c.start()


def config_spark(debug_mode: bool) -> SparkSession:
    # FIXME: disable authentication for public bucket access
    conf = SparkConf() \
        .setMaster("local[2]") \
        .setAppName("EQT ETL") \
        .set("spark.jars", "libs/gcs-connector-latest-hadoop2.jar") \
        .set(
            "spark.hadoop.google.cloud.auth.service.account.enable",
            "true"
        ) \
        .set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "libs/pyspark-gcs.json"
        )

    sc = SparkContext(conf=conf)

    if debug_mode:
        sc.setLogLevel('DEBUG')

    spark = SparkSession \
        .builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()

    # add support for gcs file system
    spark._jsc.hadoopConfiguration().set(
        'fs.gs.impl',
        'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem'
    )
    return spark


def enrich(spark, output_path):
    df_funding_rounds = (
        spark.read
        .schema(schema_funding_rounds)
        .option("mode", "FAILFAST")
        .json('gs://motherbrain-external-test/interview-test-funding.json.gz')
        .cache()
    )

    df_org = (
        spark.read
        .schema(schema_org)
        .json('gs://motherbrain-external-test/interview-test-org.json.gz')
        .cache()
    )

    df_pf = (
        spark.read
        .schema(schema_pf)
        .option("mode", "FAILFAST")
        .json(f'{DIR_PATH}/datasets/current_portfolio_scraped.json')
    )

    df_dinvestment = (
        spark.read
        .schema(schema_pf)
        .option("mode", "FAILFAST")
        .json(f'{DIR_PATH}/datasets/dinvestment_scraped.json')
    )

    df_active_funds = (
        spark.read
        .schema(schema_active_funds)
        .option("mode", "FAILFAST")
        .json(f'{DIR_PATH}/datasets/active_funds_scraped.json')
    )

    # union active and dinvested portfolio companies
    df_pf_all = dataframe_union(df_pf, df_dinvestment)

    df_pf_funding = df_pf_all.join(
        df_funding_rounds,
        how='left',
        on=('company_name')
    )

    df1 = df_pf_funding.select(
        'company_name',
        'fund',
        struct(FUNDING_COLUMNS).alias('fund_round')
    )

    df_grouped = df1.groupby('company_name', 'fund').agg(
                        collect_list('fund_round').alias('fund_rounds')
                    )

    df2 = (
        df_grouped.join(df_org, how='left', on=('company_name'))
                  .join(df_pf_all, how='left', on=(['company_name', 'fund']))
                  .join(df_active_funds, how='left', on=('fund'))
    )

    df_final = df2.select(
        'company_name',
        'fund_rounds',
        *COMPANY_DETAIL_COLUMNS,
        *FUND_DETAIL_COLUMNS
    ).sort('company_name')

    logging.info(f'------ final dataset is written to: {output_path}')
    df_final.repartition(1).write.mode('overwrite').parquet(output_path)


@click.command()
@click.option(
    '--output_path',
    default="output",
    help='Specify output file path',
    show_default=True
)
@click.option(
    '--skip_crawler',
    default=False,
    type=bool,
    help='Skip web scraping if it has done before',
    show_default=True
)
@click.option(
    '--debug_mode',
    default=False,
    type=bool,
    help='Set to true to enable spark debug log',
    show_default=True
)
def pipeline(output_path, skip_crawler, debug_mode):
    """CLI tool to enrich EQT portfolio companies data"""
    # Step 1: Crawl EQT website
    if not skip_crawler:
        run_crawler()
    # Step 2: data enrichment
    spark = config_spark(debug_mode)
    enrich(spark, output_path)
