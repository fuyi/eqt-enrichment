""" utils

This module provides utilities functions
"""

MONTHS = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12',
}


def reformat_date(date_str: str) -> str:
    """Convert string date in format 'Aug 2020' to '2020-08-01'

    Args:
        date_str: An EQT date representation. eg 'Aug 2020'
    """
    if date_str is None or date_str == '':
        return None
    [month_key, year] = date_str.split()
    return f'{year}-{MONTHS[month_key]}'
