import sys
import requests, json 

api_key = 'THISKEYHASBEENDELETEDFORPRIVACYYOUCANFINDITINTHEGOOGLEPLAYCONSOLEUNDERCREDENTIALSIFYOUDECIDETOREUSETHISCODEPLEASEPUTITINTOASEPARATEJSONTHATDOESnotBELONGINTOTHEREPO'
place_id = "ChIJyV9mg0lfqEcRnbhJji6c17E"
url = "https://maps.googleapis.com/maps/api/place/details/json?"
fields = "review" \
	+ ",international_phone_number" #justForTest

request_url = url + "key=" + api_key + "&place_id=" + place_id + "&fields=" + fields
print('I would now request: "{}"'.format(request_url))
if input('Are you sure you would like to buy data? (y) ') != "y":
	print("Bye bye bye 👋")
	sys.exit()
request = requests.get(request_url)

json = request.json()
results = json["result"]

print("Got {} results".format(len(results)))
