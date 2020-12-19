import scrapy
import json
import logging
import os

from ..utils import reformat_date

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
ROOT_PATH = DIR_PATH + '/../../..'


class EqtSpider(scrapy.Spider):
    name = 'eqt'
    allowed_domains = ['eqtgroup.com']
    start_urls = [
        'https://www.eqtgroup.com/Investments/Current-Portfolio/',
        'https://www.eqtgroup.com/Investments/Divestments/',
        'https://www.eqtgroup.com/About-EQT/Funds/Active-Funds/',
        'https://www.eqtgroup.com/About-EQT/Funds/Realized-Funds/'
    ]

    def parse(self, response):
        logging.info(f'------- start parsing: {response.url}')
        if response.url == EqtSpider.start_urls[0]:
            self._process_portfolio(response)
        if response.url == EqtSpider.start_urls[1]:
            self._process_dinvestments(response)
        if response.url == EqtSpider.start_urls[2]:
            self._process_active_funds(response)
        logging.info(f'-------- end parsing: {response.url}')

    def _process_active_funds(self, response):
        rows = response.css('tbody tr')
        records = []
        for row in rows:
            cols = row.css('td')
            record = {}
            record['fund'] = cols[0].css('a::text').get()
            record['launch_year'] = cols[1].css('td::text').get()
            record['size'] = cols[2].css('td::text').get()
            record['status'] = cols[3].css('td::text').get()
            records.append(record)
        with open(
                    f'{ROOT_PATH}/datasets/active_funds_scraped.json', 'w'
                ) as fp:
            json.dump(records, fp)

    def _process_dinvestments(self, response):
        rows = response.css('tbody tr')
        records = []
        for row in rows:
            cols = row.css('td')
            record = {}
            record['company_name'] = cols[0].css('span a::text').get()
            record['sector'] = cols[1].css('td > span::text').get()
            record['country'] = cols[2].css('td > span::text').get()
            record['fund'] = cols[3].css('td div a::text').get()
            entry = cols[4].css('td div div::text').get()
            record['entry'] = reformat_date(self._strip(entry))
            record['exit'] = reformat_date(
                cols[5].css('td div div::text').get()
            )
            records.append(record)
        with open(
                    f'{ROOT_PATH}/datasets/dinvestment_scraped.json', 'w'
                ) as fp:
            json.dump(records, fp)

    def _process_portfolio(self, response):
        rows = response.css('tr')
        rows = rows[1:]  # Get rid of header
        records = []
        for row in rows:
            cols = row.css('td')
            record = {}
            record['company_name'] = cols[0].css('span a::text').get()
            record['sector'] = cols[1].css('td > span::text').get()
            record['country'] = cols[2].css('td > span::text').get()
            record['fund'] = cols[3].css('td div a::text').get()
            entry = cols[4].css('td div div::text').get()
            record['entry'] = reformat_date(self._strip(entry))
            record['SDG'] = cols[5].css('td span::text').get().split(',')
            records.append(record)

        with open(
                    f'{ROOT_PATH}/datasets/current_portfolio_scraped.json', 'w'
                ) as fp:
            json.dump(records, fp)

    def _strip(self, obj):
        if obj is None:
            return ''
        return str(obj).strip()
