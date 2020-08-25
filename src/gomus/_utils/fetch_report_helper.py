#!/usr/bin/env python3
import csv
import datetime as dt

import requests
import xlrd

from _utils import logger

# This dict maps 'report_types' to 'REPORT_IDS'
# Data sheets that don't require a report to be generated or
# refreshed have ids <= 0
# key format: 'type_timespan' (e.g. 'customers_7days')
REPORT_IDS = {
    'customers_1day': 1364,
    'customers_7days': 1379,

    'orders_7days': 1188,
    'orders_1day': 1246,

    'entries_1day': 1262,
    'entries_unique_1day': 1509,

    'bookings_7days': 0,
    'bookings_nextYear': -5,
    'bookings_nextWeek': -6,
    'bookings_all': -11,

    'guides': -2
}


def parse_timespan(timespan, today=dt.datetime.today()):
    """
    Parse a timespan from the given string.

    The string format originates from the FetchGomusReport.suffix parameter.
    """
    end_time = today - dt.timedelta(days=1)
    if timespan == '7days':
        # grab everything from yesterday till a week before
        start_time = end_time - dt.timedelta(weeks=1)
    elif timespan == '1year':
        start_time = end_time - dt.timedelta(days=365)
    elif timespan == '1day':
        start_time = end_time
    elif timespan == 'nextYear':
        start_time = end_time
        end_time = end_time + dt.timedelta(days=365)
    elif timespan == 'nextWeek':
        start_time = end_time
        end_time = end_time + dt.timedelta(days=7)
    elif timespan == 'all':
        start_time = today - dt.timedelta(days=365 * 5)
        end_time = today + dt.timedelta(days=365 * 2)
    else:
        start_time = dt.date.min  # check this for error handling
    return start_time, end_time


def csv_from_excel(xlsx_content, target_csv, sheet_index):
    """Extract a sheet from a Microsoft Excel (XLSX) file into a CSV file."""
    workbook = xlrd.open_workbook(file_contents=xlsx_content)
    sheet = workbook.sheet_by_index(sheet_index)
    writer = csv.writer(target_csv, quoting=csv.QUOTE_NONNUMERIC)
    for row_num in range(sheet.nrows):
        writer.writerow(sheet.row_values(row_num))


def direct_download_url(base_url, report, timespan):
    """Generate download URL for a gomus report."""
    start_time, end_time = parse_timespan(timespan)
    base_return = base_url + f'/{report}.xlsx'

    if start_time == dt.date.min:
        return base_return

    # timespan is valid
    end_time = end_time.strftime("%Y-%m-%d")
    start_time = start_time.strftime("%Y-%m-%d")
    logger.info(f"Requesting report for timespan "
                f"from {start_time} to {end_time}")
    return f'{base_return}?end_at={end_time}&start_at={start_time}'


def get_request(url, sess_id):
    """Request the given URL from the gomus servers and return the results."""
    cookies = dict(_session_id=sess_id)
    response = requests.get(url, cookies=cookies)
    response.raise_for_status()
    if response.ok:
        logger.info("HTTP request successful")

    return response.content


def request_report(report_type, session_id):
    """Download a generated report from the Gomus servers."""
    base_url = 'https://barberini.gomus.de'
    report_parts = report_type.split("_")
    report_id = REPORT_IDS[report_type]

    logger.info(f"Working with report '{report_parts[0]}.xlsx'")

    # Work with the kind of report that is generated and maintained
    if report_id > 0:
        base_url += f'/admin/reports/{report_id}'

        logger.info("Fetching report")
        url = base_url + '.xlsx'

    else:  # Work with the kind of report that is requested directly
        logger.info("Directly downloading report")
        if len(report_parts) < 2:
            timespan = ''
        else:
            timespan = report_parts[1]

        url = direct_download_url(base_url, report_parts[0], timespan)

    return get_request(url, session_id)
