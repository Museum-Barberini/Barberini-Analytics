import os
import pprint
import json

import google.oauth2.credentials
import oauth2client
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

pp = pprint.PrettyPrinter(indent=2)

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRET_FILE = "client_secret_other.json"

# This access scope grants read-only access to the authenticated user's Drive
# account.
with open(CLIENT_SECRET_FILE) as secret_file:
	secret = json.load(secret_file)['installed']
SCOPES = ['https://www.googleapis.com/auth/business.manage']
API_SERVICE_NAME = 'mybusiness'
API_VERSION = 'v4'


def get_authenticated_service():
	# Create a credential storage object.  You pick the filename.
	storage = Storage('bankOfLon.don')
	
	# Attempt to load existing credentials.  Null is returned if it fails.
	credentials = storage.get()
	
	# Only attempt to get new credentials if the load failed.
	if not credentials:
		# Run through the OAuth flow and retrieve credentials                                                                                 
		flow = OAuth2WebServerFlow(secret['client_id'], secret['client_secret'], SCOPES, secret['redirect_uris'][0])
		
		authorize_url = flow.step1_get_authorize_url()
		print('Go to the following link in your browser: ' + authorize_url)
		code = input('Enter verification code: ').strip()
		
		credentials = flow.step2_exchange(code)
		storage.put(credentials)
	return build(API_SERVICE_NAME, API_VERSION, credentials = credentials, 
		discoveryServiceUrl='https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p5.json')

def list_drive_files(service, **kwargs):
	results = service.accounts().locations().reviews().list(#.locations().reviews()
		**kwargs).execute()
	
	pp.pprint(results)

if __name__ == '__main__':
	# When running locally, disable OAuthlib's HTTPs verification. When
	# running in production *do not* leave this option enabled.
	os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
	service = get_authenticated_service()
	list_drive_files(service,
		#parent='accounts/117572894115944798318'
		pageSize=50, parent='accounts/117572894115944798318/locations/2567716408507749660'
	)
