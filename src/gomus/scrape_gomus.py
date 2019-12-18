import luigi
from luigi.format import UTF8
import os
import requests
from lxml import html
import pandas as pd
import csv
import dateparser

class ScrapeGomusBookings(luigi.Task):
	

	base_url = "https://barberini.gomus.de"

	# this URL filters for non-stornierte Bookings
	url_appendment = "/bookings?end_at=2020-01-01&start_at=2019-12-18"

	sess_id = os.environ['GOMUS_SESS_ID']
	cookies = dict(_session_id=sess_id)

	

	def extract_from_html(self, base_html, xpath):
		try:
			return html.tostring(base_html.xpath(xpath)[0], method='text', encoding="unicode")
		except IndexError as err:
			return ""
	

	# returns url-appendment for next page if one exists
	def fetch_page_of_bookings(self, url, output_buffer):
		res_bookings = requests.get(url, cookies=self.cookies)

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
				res_details = requests.get(booking_url, cookies=self.cookies)

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


	def output(self):
		return luigi.LocalTarget('output/gomus/scraped_bookings.csv', format=UTF8)


	def run(self):
		booking_data = []

		next_page_appendment = self.fetch_page_of_bookings(self.base_url + self.url_appendment, booking_data)
		while (next_page_appendment != None):
			next_page_appendment = self.fetch_page_of_bookings(self.base_url + next_page_appendment, booking_data)


		df = pd.DataFrame([data_entry for data_entry in booking_data])
		with self.output().open('w') as output_file:
			df.to_csv(output_file, index=False, header=True, quoting=csv.QUOTE_NONNUMERIC)