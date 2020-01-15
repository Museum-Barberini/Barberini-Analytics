import os
import pprint
import json

import google.oauth2.credentials
import oauth2client
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

pp = pprint.PrettyPrinter(indent=2)

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "client_secret_other.json"

# This access scope grants read-only access to the authenticated user's Drive
# account.
SCOPES = ['https://www.googleapis.com/auth/business.manage']
API_SERVICE_NAME = 'mybusiness'
API_VERSION = 'v4'


def get_authenticated_service():
	#flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
	#credentials = flow.run_console()
	credentials = oauth2client.client.GoogleCredentials.new_from_json(open(CLIENT_SECRETS_FILE).read())
	oauth2client.client.save_to_well_known_file(credentials, os.path.abspath('saved_secrets.json'))
	return build(API_SERVICE_NAME, API_VERSION, credentials = credentials, 
		discoveryServiceUrl='https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p5.json')

def list_drive_files(service, **kwargs):
	results = service.accounts().locations().reviews().list(
	**kwargs
	).execute()
	
	pp.pprint(results)

if __name__ == '__main__':
	# When running locally, disable OAuthlib's HTTPs verification. When
	# running in production *do not* leave this option enabled.
	os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
	service = get_authenticated_service()
	list_drive_files(service, 
		pageSize=50, parent='accounts/117572894115944798318/locations/2567716408507749660')
