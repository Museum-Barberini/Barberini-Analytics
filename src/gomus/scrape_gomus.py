import luigi
from luigi.format import UTF8
import os
import requests
from lxml import html
import pandas as pd
import csv
import dateparser
import datetime as dt
import time
from set_db_connection_options import set_db_connection_options
import psycopg2


class ScrapeGomusBookings(luigi.Task):
	

	base_url = "https://barberini.gomus.de"

	# this URL filters for non-stornierte Bookings
	

	sess_id = os.environ['GOMUS_SESS_ID']
	cookies = dict(_session_id=sess_id)

	host     = None
	database = None
	user     = None
	password = None

	# simply wait for a moment before requesting, as we don't want to overwhealm the server with our interest in classified information...
	def politeGet(self, url, cookies):
		time.sleep(0.5)
		return requests.get(url, cookies=cookies)



	def extract_from_html(self, base_html, xpath):
		try:
			return html.tostring(base_html.xpath(xpath)[0], method='text', encoding="unicode")
		except IndexError as err:
			return ""
	

	# returns url-appendment for next page if one exists
	def fetch_page_of_bookings(self, url, output_buffer):
		print("Requesting: " + url)
		res_bookings = self.politeGet(url, cookies=self.cookies)

		if not res_bookings.status_code == 200:
			print(f'Error with HTTP request: Status code {res_bookings.status_code}')
			exit(1)
		else:
			print('HTTP request successful')

		tree_bookings = html.fromstring(res_bookings.text)
		for day in tree_bookings.xpath('//body/div[2]/div[2]/div[3]/div/div[2]/div/div[2]/table'):
			for booking in day.xpath('tbody/tr'):
				new_booking = dict()

				# ID
				booking_id = int(self.extract_from_html(booking, 'td[1]/a'))
				new_booking['id'] = booking_id


				booking_url = "https://barberini.gomus.de/admin/bookings/" + str(booking_id)
				res_details = self.politeGet(booking_url, cookies=self.cookies)

				tree_details = html.fromstring(res_details.text)
				
				# firefox says the the xpath starts with //body/div[3]/ but we apparently need div[2] instead
				booking_details = tree_details.xpath('//body/div[2]/div[2]/div[3]/div[4]/div[2]/div[1]/div[3]')[0]

				print("requesting booking details for id: " + str(booking_id))
				
				# Order Date
				raw_order_date = self.extract_from_html(booking_details, 'div[1]/div[2]/small/dl/dd[2]').strip() # removes \n in front of and behind string
				new_booking['order_date'] = dateparser.parse(raw_order_date)
				
				# Language
				new_booking['language'] = self.extract_from_html(booking_details, 'div[3]/div[1]/dl[2]/dd[1]').strip()
				
				output_buffer.append(new_booking)

		potential_appendment = tree_bookings.xpath("//li[@class='next_page']/a/@href")
		print(potential_appendment)
		if (potential_appendment.__len__() > 0):
			return potential_appendment[0]
		else:
			return None


	def getLatestOrderDateFromDB(self):
		#return dt.date.today()
		alternate_date = '2016-08-01'
		
		try:
			conn = psycopg2.connect(
				host=self.host, database=self.database,
				user=self.user, password=self.password
			)
			cur = conn.cursor()
			cur.execute(f"SELECT MAX(order_date) FROM gomus_bookings") # TODO: use order_date instead of date!
			return cur.fetchone()[0] or alternate_date
			conn.close()
		
		except psycopg2.DatabaseError as error:
			print(error)
			if conn is not None:
				conn.close()
			return alternate_date


	def output(self):
		return luigi.LocalTarget('output/gomus/scraped_bookings.csv', format=UTF8)


	def run(self):
		set_db_connection_options(self)
		booking_data = []

		startDateStr = self.getLatestOrderDateFromDB()
		endDateStr = dt.date.today() + dt.timedelta(weeks=12)

		url_appendment = f"/bookings?end_at={endDateStr}&start_at={startDateStr}"

		next_page_appendment = self.fetch_page_of_bookings(self.base_url + url_appendment, booking_data)
		while (next_page_appendment != None):
			next_page_appendment = self.fetch_page_of_bookings(self.base_url + next_page_appendment, booking_data)


		df = pd.DataFrame([data_entry for data_entry in booking_data])
		with self.output().open('w') as output_file:
			df.to_csv(output_file, index=False, header=True, quoting=csv.QUOTE_NONNUMERIC)