import requests
import pandas as pd
import luigi
from csv_to_db import CsvToDb
from luigi.format import UTF8
import json
from lxml import html
import xmltodict

######### TARGETS ############

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
        ("id", "TEXT"),
        ("content", "TEXT"),
        ("rating", "INT"),
        ("app_version", "TEXT"),
        ("vote_count", "INT"),
        ("vote_sum", "INT"),
        ("title", "TEXT"),
        ("date", "DATE"),
        ("country_code", "TEXT")
    ]
    
    primary_key = "id"
    
    def requires(self):
        return FetchAppstoreReviews()



######### FETCH LOGIC ###########

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
    return ret.drop_duplicates()

def get_country_codes():
    return requests.get("http://country.io/names.json").json().keys()

def fetch_country(country_code):
    with open('data/barberini-facts.json') as facts_json:
        barberini_facts = json.load(facts_json)
        app_id = barberini_facts['ids']['apple']['appId']
    url = f"https://itunes.apple.com/{country_code}/rss/customerreviews/page=1/id={app_id}/sortby=mostrecent/xml"
    data_list = []
    
    while True:
        try:
            data, response = fetch_single_url(url)
        except StopIteration:
            break
        
        data_list = [*data_list, *data]
        next_page = filter(lambda x: x["attributes"]["rel"] == "next", response["link"])
        url = next(next_page)["attributes"]["href"]
    
    if len(data_list) == 0:
        # no reviews for the given country code
        raise ValueError()
        
    result = pd.DataFrame(data_list)
    result["country_code"] = country_code
    
    return result

def fetch_single_url(url):
    
    response = requests.get(url)
    
    if response.ok is False:
        raise requests.HTTPError
    
    response_content = xmltodict.parse(response.text)["feed"]
    
    if "entry" not in response_content:
        raise StopIteration()
        
    entries = response_content["entry"]
    import pdb;pdb.set_trace()
    if isinstance(entries, dict):
        entries = [entries]
    data = [{
        "id": item["id"], 
        "content": item["content"], 
        "rating": item["im:rating"],
        "app_version": item["im:version"],
        "vote_count": item["im:voteCount"],
        "vote_sum": item["im:voteSum"],
        "title": item["title"],
        "date": item["updated"]
    } for item in entries]
    
    return data, response_content
