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


def enrich(output_path):
    spark = SparkSession \
        .builder \
        .appName("EQT funding") \
        .getOrCreate()

    df_funding_rounds = (
        spark.read
        .schema(schema_funding_rounds)
        .json(f'{DIR_PATH}/datasets/interview-test-funding.json')
    )

    df_org = (
        spark.read
        .schema(schema_org)
        .json(f'{DIR_PATH}/datasets/interview-test-org.json')
    )

    df_pf = (
        spark.read
        .schema(schema_pf)
        .json(f'{DIR_PATH}/datasets/current_portfolio_scraped.json')
    )

    df_dinvestment = (
        spark.read
        .schema(schema_pf)
        .json(f'{DIR_PATH}/datasets/dinvestment_scraped.json')
    )

    df_active_funds = (
        spark.read
        .schema(schema_active_funds)
        .json(f'{DIR_PATH}/datasets/active_funds_scraped.json')
    )

    # union active and dinvested portfolio companies
    df_pf_all = df_pf.union(df_dinvestment)

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

    # df_final.show(truncate=False)

    # import pdb; pdb.set_trace()
    logging.info(f'------ final dataset is written to: {output_path}')
    df_final.write.mode('overwrite').parquet(output_path)
    # df_final.write.mode('overwrite').parquet('/Users/yi.fu/yi-ouput-absolute')


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
def pipeline(output_path, skip_crawler):
    """CLI tool to enrich EQT portfolio companies data"""
    # Step 1: Crawl EQT website
    if not skip_crawler:
        run_crawler()
    # Step 2: data enrichment
    enrich(output_path)
