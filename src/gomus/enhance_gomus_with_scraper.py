import luigi
from luigi.format import UTF8
import pandas as pd
import csv
from extract_gomus_data import ExtractGomusBookings
from scrape_gomus import ScrapeGomusBookings


class EnhanceBookingsWithScraper(luigi.Task):

	def requires(self):
        yield ExtractGomusBookings()
        yield ScrapeGomusBookings()


	def run(self):
		fetched_bookings = pd.read_csv(self.input()[0].path, index_col='id')
		scraped_bookings = pd.read_csv(self.input()[1].path, index_col='id')

		fetched_bookings = fetched_bookings.join(scraped_bookings)
		with self.output().open('w') as output_file:
			fetched_bookings.to_csv(output_file, header=True)

		


	def output(self):
		return luigi.LocalTarget(f'output/gomus/bookings.csv', format=UTF8)