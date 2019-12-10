import luigi
import psycopg2
from luigi.format import UTF8
import datetime as dt
import pandas as pd
import requests
import json

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options


class FetchFbPosts(luigi.Task):

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)
	
	def output(self):
		return luigi.LocalTarget("output/facebook/fb_posts.csv", format=UTF8)
	
	def run(self):
		
		# DO NOT COMMIT THE ACCESS TOKEN
		access_token = "WE-NEED-A-WAY-TO-STORE-ACCESS-TOKENS-OUTSIDE-AND-STILL-GET-THEM-IN-HERE"
		
		posts = []
		
		url = "https://graph.facebook.com/1068121123268077/posts?access_token=" + access_token
		
		response = requests.get(url)
		
		if response.ok is False:
			raise requests.HTTPError
		
		response_content = response.json()
		for post in (response_content['data']):
			posts.append(post)
		
		while ('next' in response_content['paging']):
			print("### Facebook - loading new page of posts ###")
			url = response_content['paging']['next']
			response = requests.get(url)
			
			if response.ok is False:
				raise requests.HTTPError
			
			response_content = response.json()
			for post in (response_content['data']):
				posts.append(post)
		
		
		with self.output().open('w') as output_file:
			df = pd.DataFrame([post for post in posts])
			df = df.filter(['created_time', 'message', 'id'])
			df.columns = ['post_date', 'text', 'id']
			df.to_csv(output_file, index=False, header=True)


class FetchFbPostPerformance(luigi.Task):
	
	def requires(self):
		return FetchFbPosts()
	
	def output(self):
		return luigi.LocalTarget("output/facebook/fb_post_performances.csv", format=UTF8)
	
	def run(self):
		
		# DO NOT COMMIT THE ACCESS TOKEN
		access_token = "WE-NEED-A-WAY-TO-STORE-ACCESS-TOKENS-OUTSIDE-AND-STILL-GET-THEM-IN-HERE"
		
		
		current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		
		performances = []
		
		df = pd.read_csv(self.input().path)
		#df = df.filter(["id"])
		for index in df.index:
			post_id = df['id'][index]
			print(f"### Facebook - loading performance data for post {str(post_id)} ###")
			url = f"https://graph.facebook.com/{post_id}/insights?access_token={access_token}&metric=post_reactions_by_type_total,post_activity_by_action_type,post_clicks_by_type,post_negative_feedback"
			response = requests.get(url)
			
			if response.ok is False:
				raise requests.HTTPError
			
			response_content = response.json()
			
			post_perf = dict()
			post_perf["post_id"] = post_id
			post_perf["time_stamp"] = current_timestamp
			
			#Reactions
			reactions = response_content['data'][0]['values'][0]['value']
			post_perf["react_like"] = int(reactions.get('like', 0))
			post_perf["react_love"] = int(reactions.get('love', 0))
			post_perf["react_wow"] = int(reactions.get('wow', 0))
			post_perf["react_haha"] = int(reactions.get('haha', 0))
			post_perf["react_sorry"] = int(reactions.get('sorry', 0))
			post_perf["react_anger"] = int(reactions.get('anger', 0))
			
			#Activity
			activity = response_content['data'][1]['values'][0]['value']
			post_perf["likes"] = int(activity.get('like', 0))
			post_perf["shares"] = int(activity.get('share', 0))
			post_perf["comments"] = int(activity.get('comment', 0))

			#Clicks
			clicks = response_content['data'][2]['values'][0]['value']
			post_perf["video_clicks"] = int(clicks.get('video play', 0))
			post_perf["link_clicks"] = int(clicks.get('link clicks', 0))
			post_perf["other_clicks"] = int(clicks.get('other clicks', 0))
			
			#negative feedback (only one field)
			post_perf["negative_feedback"] = clicks = response_content['data'][3]['values'][0]['value']
			
			performances.append(post_perf)
		
		with self.output().open('w') as output_file:
			df = pd.DataFrame([perf for perf in performances])
			df.to_csv(output_file, index=False, header=True)




class FbPostsToDB(CsvToDb):
	
	table = "fb_post"
	
	columns = [
		("post_date", "TIMESTAMP"),
		("text", "TEXT"),
		("id", "TEXT")
	]
	
	def requires(self):
		return FetchFbPosts()
 

class FbPostPerformanceToDB(CsvToDb):
	
	table = "fb_post_performance"
	
	columns = [
		("post_id", "TEXT"),
		("time_stamp", "TIMESTAMP"),
		("react_like", "INT"),
		("react_love", "INT"),
		("react_wow", "INT"),
		("react_haha", "INT"),
		("react_sorry", "INT"),
		("react_anger", "INT"),
		("likes", "INT"),
		("shares", "INT"),
		("comments", "INT"),
		("video_clicks", "INT"),
		("link_clicks", "INT"),
		("other_clicks", "INT"),
		("negative_feedback", "INT")
	]
	
	def requires(self):
		return FetchFbPostPerformance()
