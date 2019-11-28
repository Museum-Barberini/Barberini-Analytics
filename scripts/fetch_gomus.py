#!/usr/bin/env python3
import argparse
import datetime
import csv
import requests
import sys
import xlrd

# This dict maps 'report_types' to 'report_ids'
# Data sheets that don't require a report to be generated or refreshed have ids <= 0
# key format: 'type_timespan' (e.g. 'customers_7days')
report_ids = {
    'customers_7days': 1186,
    'orders_7days': 1188,
    'bookings_7days': 0
}

def parse_arguments(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Refresh and fetch reports from go~mus')
    report_group = parser.add_mutually_exclusive_group(required=True)

    report_group.add_argument('-i', '--report-id', type=int, help='ID of the report', choices=report_ids.values())
    report_group.add_argument('-t', '--report-type', type=str, help='Type of the report', choices=report_ids.keys())

    parser.add_argument('action', type=str, help='Action to take', choices=['refresh', 'fetch'], nargs='?', default='fetch')
    parser.add_argument('-s', '--session-id', type=str, help='Session ID to use for authentication', required=True)

    parser.add_argument('-o', '--output-file', type=str, help='Name of Output file (for fetching)')

    return parser.parse_args(args)
            
def direct_download_url(base_url, report, timespan):
    today = datetime.date.today()
    if timespan == '7days': # grab everything from yesterday till a week before
        end_time = today - datetime.timedelta(days=1)
        start_time = today - datetime.timedelta(weeks=1)
    
    end_time = end_time.strftime("%Y-%m-%d")
    start_time = start_time.strftime("%Y-%m-%d")
    print(f'Requesting report for timespan from {start_time} to {end_time}')

    return base_url + f'/{report}.xlsx?end_at={end_time}&start_at={start_time}'

def get_request(url, sess_id):
    cookies = dict(_session_id=sess_id)
    req = requests.get(url, cookies=cookies)
    if not req.status_code == 200:
        print(f'Error with HTTP request: Status code {req.status_code}')
        exit(1)
    else:
        print('HTTP request successful')
    
    return req.content

def csv_from_excel(xlsx_content, target_csv):
    workbook = xlrd.open_workbook(file_contents=xlsx_content)
    sheet = workbook.sheet_by_index(0)
    with open(target_csv, 'w') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)
        for row_num in range(sheet.nrows):
            writer.writerow(sheet.row_values(row_num))

def request_report():
    args = parse_arguments()
    if args.report_id:
        report_id = args.report_id
    else:
        try:
            report_id = report_ids[args.report_type]
        except KeyError: # should never happen because of argparse choices
            print(f'Error: Report type \'{args.report_type}\' not supported!')
            exit(1)

    base_url = 'https://barberini.gomus.de'

    report_ids_inv = {v: k for k, v in report_ids.items()}
    report_parts = report_ids_inv[report_id].split("_")
    
    print(f'Working with report "{report_parts[0]}.xlsx"')

    if report_id > 0: # Work with the kind of report that is generated and maintained
        base_url += f'/admin/reports/{report_id}'

        if args.action == 'refresh':
            print('Refreshing report')
            url = base_url + '/refresh'

        elif args.action == 'fetch':
            print('Fetching report')
            url = base_url + '.xlsx'

    else: # Work with the kind of report that is requested directly
        print('Directly downloading report')
        if args.action == 'refresh':
            print('Error: Directly downloaded reports cannot be refreshed')
            exit(1)
        url = direct_download_url(base_url, report_parts[0], report_parts[1])
    
    req_content = get_request(url, args.session_id)

    if args.action == 'fetch':
        filename = args.output_file
        if not filename: filename = report_ids_inv[report_id] + '.csv'
        csv_from_excel(req_content, filename)
        print(f'Saved report to file "{filename}"')

if __name__ == '__main__':
    request_report()