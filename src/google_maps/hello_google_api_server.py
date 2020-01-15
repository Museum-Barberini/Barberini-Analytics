import os
import pprint

import google.oauth2.credentials

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account

pp = pprint.PrettyPrinter(indent=2)

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
#CLIENT_SECRETS_FILE = "client_secret_web.json"

# This access scope grants read-only access to the authenticated user's Drive
# account.
SCOPES = ['https://www.googleapis.com/auth/business.manage']
API_SERVICE_NAME = 'mybusiness'
API_VERSION = 'v4'

SERVICE_ACCOUNT_FILE = 'server_secret.json'

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

def get_authenticated_service():
	return build(API_SERVICE_NAME, API_VERSION, credentials = credentials, 
		discoveryServiceUrl='https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p5.json')

def list_drive_files(service, **kwargs):
	results = service.accounts().locations().list(#reviews().list(
	**kwargs
	).execute()

	pp.pprint(results)

if __name__ == '__main__':
	# When running locally, disable OAuthlib's HTTPs verification. When
	# running in production *do not* leave this option enabled.
	#os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
	service = get_authenticated_service()
	list_drive_files(service, 
		pageSize=50, parent='accounts/112621258378376648266')#/locations/2567716408507749660')
