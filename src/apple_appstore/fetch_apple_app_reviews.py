import luigi
import requests
import pandas as pd
import json
from museum_facts import MuseumFacts
from csv_to_db import CsvToDb
from luigi.format import UTF8


class FetchAppstoreReviews(luigi.Task):

    def output(self):
        return luigi.LocalTarget("output/appstore_reviews.csv", format=UTF8)

    def run(self):
        reviews = fetch_all()
        with self.output().open('w') as output_file:
            reviews.to_csv(output_file, index=False, header=True)


class AppstoreReviewsToDB(CsvToDb):

    table = "appstore_review"

    columns = [
        ("appstore_review_id", "TEXT"),
        ("text", "TEXT"),
        ("content_type", "TEXT"),
        ("rating", "INT"),
        ("app_version", "TEXT"),
        ("vote_count", "INT"),
        ("vote_sum", "INT"),
        ("title", "TEXT")
        # TODO: retrieve review date
        #("date", "DATE")
    ]

    primary_key = "appstore_review_id"

    def requires(self):
        return FetchAppstoreReviews()


def fetch_all():
    data = []
    country_codes = get_country_codes()
    for country_code in country_codes:
        try:
            data = [*data, fetch_country(country_code)]
        except ValueError:
            pass  # no data for given country code
        except requests.HTTPError:
            pass
    ret = pd.concat(data)
    ret = ret.drop('countryId', axis=1)
    return ret.drop_duplicates()


def get_country_codes():
    return requests.get("http://country.io/names.json").json().keys()


def fetch_country(country_id):
    # TODO!
    facts = MuseumFacts()
    facts.run()
    museum_facts = json.load(facts.output().open('r'))
    # Hacked! Once !38 has been merged, require MuseumFacts instead!

    app_id = museum_facts['ids']['apple']['appId']
    url = f"https://itunes.apple.com/{country_id}/rss/customerreviews/page=1/id={app_id}/sortby=mostrecent/json"
    data_list = []

    while True:
        url = url.replace("xml", "json")

        try:
            data, response = fetch_single_url(url)
        except StopIteration:
            break

        data_list = [*data_list, *data]
        next_page = filter(lambda x: x["attributes"]["rel"] == "next", response["feed"]["link"])
        url = next(next_page)["attributes"]["href"]

    if len(data_list) == 0:
        # no reviews for the given country code
        raise ValueError()

    result = pd.DataFrame(data_list)
    result["countryId"] = country_id

    return result


def fetch_single_url(url):
    response = requests.get(url)
    if response.ok is False:
        raise requests.HTTPError
    response_content = response.json()
    if "entry" not in response_content["feed"]:
        raise StopIteration()

    entries = response_content["feed"]["entry"]
    if isinstance(entries, dict):
        entries = [entries]
    data = [{
        "appstore_review_id": item["id"]["label"],
        "text": item["content"]["label"],
        "content_type": item["content"]["attributes"]["type"],
        "rating": item["im:rating"]["label"],
        "app_version": item["im:version"]["label"],
        "vote_count": item["im:voteCount"]["label"],
        "vote_sum": item["im:voteSum"]["label"],
        "title": item["title"]["label"]
    } for item in entries]

    return data, response_content
