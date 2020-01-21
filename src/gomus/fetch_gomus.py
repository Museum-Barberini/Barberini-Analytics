#!/usr/bin/env python3
##########################################
##       ###      ###   ####        ## ###
####  ###### #### ### #   ## ###### ## ###
####  ###### #### ### ##  ## ###### ## ###
####  ###### #### ### #   ## ###### ######
####  ######      ###   ####        ## ###
##########################################
# TODOs for the next editor:
# - Merge this file with the calling task. We decided not to have separate scripts for things that represent a task.
# ct.

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
	'customers_7days': 1226,
	'orders_7days': 1188,
	'orders_1day': 1246,
	'bookings_7days': 0,

	'bookings_1month': -3,

	'bookings_1year': -1,
	
	'guides': -2
}

def parse_arguments(args):
	parser = argparse.ArgumentParser(description="Refresh and fetch reports from go~mus")
	report_group = parser.add_mutually_exclusive_group(required=True)

	report_group.add_argument('-i', '--report-id', type=int, help='ID of the report', choices=report_ids.values())
	report_group.add_argument('-t', '--report-type', type=str, help='Type of the report', choices=report_ids.keys())

	parser.add_argument('action', type=str, help='Action to take', choices=['refresh', 'fetch'], nargs='?', default='fetch')
	parser.add_argument('-s', '--session-id', type=str, help='Session ID to use for authentication', required=True)

	parser.add_argument('-I', '--sheet-index', type=int, help="Excel sheet page number", default=0)

	parser.add_argument('-o', '--output-file', type=str, help='Name of Output file (for fetching)')

	parser.add_argument('-l', '--luigi', help='Set true if run as part of a Luigi task', action='store_true')

	return parser.parse_args(args)
			
def direct_download_url(base_url, report, timespan):
	no_time = False
	today = datetime.date.today()
	end_time = today - datetime.timedelta(days=1)
	if timespan == '7days': # grab everything from yesterday till a week before
		start_time = today - datetime.timedelta(weeks=1)
	elif timespan == '1month':
		start_time = today - datetime.timedelta(days=30)
	elif timespan == '1year':
		start_time = today - datetime.timedelta(days=365)
	elif timespan == '1day':
		start_time = end_time
	else: no_time = True
	
	base_return = base_url + f'/{report}.xlsx'
	
	if not no_time:
		end_time = end_time.strftime("%Y-%m-%d")
		start_time = start_time.strftime("%Y-%m-%d")
		print(f"Requesting report for timespan from {start_time} to {end_time}")
		return base_return + f'?end_at={end_time}&start_at={start_time}'
	
	return base_return
	

def get_request(url, sess_id):
	cookies = dict(_session_id=sess_id)
	res = requests.get(url, cookies=cookies)
	if not res.status_code == 200:
		print(f"Error with HTTP request: Status code {res.status_code}")
		exit(1)
	else:
		print("HTTP request successful")
	
	return res.content

def csv_from_excel(xlsx_content, target_csv, sheet_index):
	workbook = xlrd.open_workbook(file_contents=xlsx_content)
	sheet = workbook.sheet_by_index(sheet_index)
	writer = csv.writer(target_csv, quoting=csv.QUOTE_NONNUMERIC)
	for row_num in range(sheet.nrows):
		writer.writerow(sheet.row_values(row_num))

def request_report(args=sys.argv[1:]):
	args = parse_arguments(args)
	if args.report_id:
		report_id = args.report_id
	else:
		try:
			report_id = report_ids[args.report_type]
		except KeyError: # should never happen because of argparse choices
			print(f"Error: Report type '{args.report_type}' not supported!")
			exit(1)
	
	base_url = 'https://barberini.gomus.de'
	
	report_ids_inv = {v: k for k, v in report_ids.items()}
	report_parts = report_ids_inv[report_id].split("_")
	
	print(f"Working with report '{report_parts[0]}.xlsx'")
	
	if report_id > 0: # Work with the kind of report that is generated and maintained
		base_url += f'/admin/reports/{report_id}'
		
		if args.action == 'refresh':
			print("Refreshing report")
			url = base_url + '/refresh'
		
		elif args.action == 'fetch':
			print("Fetching report")
			url = base_url + '.xlsx'

	else: # Work with the kind of report that is requested directly
		print("Directly downloading report")
		if args.action == 'refresh':
			print("Error: Directly downloaded reports cannot be refreshed")
			exit(1)
		if len(report_parts) < 2:
			timespan = ''
		else:
			timespan = report_parts[1]
		
		url = direct_download_url(base_url, report_parts[0], timespan)
	
	res_content = get_request(url, args.session_id)

	if args.action == 'fetch':
		if not args.luigi:
			filename = args.output_file
			if not filename: filename = report_ids_inv[report_id] + '.csv'
			with open(filename, 'w', encoding='utf-8') as csv_file:
				csv_from_excel(res_content, csv_file, args.sheet_index)
			print(f'Saved report to file "{filename}"')
		else:
			print("Running as Luigi task, returning response content")
			return res_content

if __name__ == '__main__':
	request_report()
