#-------------------------------------------------------------------------------
# Name:        PSE Power Outages Data Extraction
# Purpose:  This script extracts all power outages from the PSE provided online
#           map and applies it to ArcGIS Online.
#
# Author:      John Spence
#
#  https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
#
#
# Created:
# Modified:
# Modification Purpose:
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
#   To be completed.
#
# ------------------------------- Dependencies ---------------------------------
# 1) Using PIP, install PyODBC if you have not previously.
# 2) This script assumes you are using MS SQL as your RDBMS.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Puget Sound Eneregy Data Source
pse_status = 'https://www.pse.com/api/sitecore/OutageMap/AnonymoussMapListView'

# ArcGIS Online Portal
AGOL_Portal = ''  #If blank, it will search for the active portal from your ArcGIS//Pro install

# Targeted Service & layer for Data
service_URL = 'https://services1.arcgis.com/YourPointsService'
service_URL_Area = 'https://services1.arcgis.com/YourAreaService'

# ArcGIS Online Credentials
AGOL_User = 'UserName'
AGOL_Pass = 'Password'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import urllib
import time
import datetime
import re
import requests, json, collections, string

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def get_token():

    url = 'https://www.arcgis.com/sharing/rest/generateToken'
    values = {'f': 'json',
              'username': AGOL_User,
              'password': AGOL_Pass,
              'referer' : 'https://www.arcgis.com',
              'expiration' : '10'}

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url)

    response = None
    while response is None:
        try:
            response = urllib.request.urlopen(req,data=data)
        except:
            pass
    the_page = response.read()

    #Garbage Collection with some house building
    payload_json = the_page.decode('utf8')
    payload_json = json.loads(payload_json)

    edit_token = payload_json['token']

    return (edit_token)

def getPSE():
    status_response = requests.get (pse_status)
    status_data = status_response.json()

    status_payload = status_data['PseMap']

    print (status_payload)

    for item in status_payload:
        pse_poly = []
        status_message = item['DataProvider']['Attributes']
        poi_data = item['DataProvider']['PointOfInterest']
        polygon_data = item['Polygon']
        print ('ID: ' + poi_data['Id'])
        outage_id = poi_data['Id']
        print ('Title: ' + poi_data['Title'])
        outage_title = poi_data['Title']
        print ('	Map Type: ' + poi_data['MapType'])
        outage_mapType = poi_data['MapType']
        print ('	Pin Type: ' + poi_data['PinType'])
        outage_pinType = poi_data['PinType']
        print ('	Planned Outage: ' + str(poi_data['PlannedOutage']))
        if poi_data['PlannedOutage'] == True:
            output_plannedoutage = 'Yes'
        else:
            output_plannedoutage = 'No'

        for message in status_message:
            if message['Name'] == 'Est. restoration time':
                check_field = 1
                break
            else:
                output_estrestore = 'TBD'

        for message in status_message:
            print ('	' + message['Name'] + ': ' + message['Value'])
            if message['Name'] == 'Start time':
                output_starttime = message['Value']
            elif message['Name'] == 'Est. restoration time':
                output_estrestore = message['Value']
            elif message['Name'] == 'Customers impacted':
                output_impact = message['Value']
            elif message['Name'] == 'Cause':
                output_cause = message['Value']
            elif message['Name'] == 'Status':
                output_status = message['Value']
            elif message['Name'] == 'Last updated':
                output_updatetime = message['Value']
            else:
                print ('		***UNKNOWN FIELD ID:  ' + message['Name'])

        print ('	Longitude: ' + poi_data['Longitude'])
        output_long_pt = float(poi_data['Longitude'])
        print ('	Latitude: ' + poi_data['Latitude'])
        output_lat_pt = float(poi_data['Latitude'])
        print('\n	Polygon:')
        init = 0
        for tp in polygon_data:
            temp_poly = []
            print ('	  ' + tp['Longitude'] + ', ' + tp['Latitude'])
            temp_poly.append(float(tp['Longitude']))
            temp_poly.append(float(tp['Latitude']))
            pse_poly.append(temp_poly)

        #print (pse_poly)

        pushToAGOL(outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage, output_starttime,
                   output_estrestore, output_impact, output_cause, output_status, output_updatetime, output_long_pt, output_lat_pt, pse_poly)

def queryCount(outage_id,edit_token):

    edit_token = get_token()

    FS_service = service_URL + 'query/?token={}'.format(edit_token)

    where_statement = 'ID=\'{}\''.format(outage_id)

    data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'returnCountOnly': 'true'}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    response_payload = response.read()
    response_payload = json.loads(response_payload)
    item_count = response_payload['count']

    if item_count > 0:
        where_statement = 'ID=\'{}\''.format(outage_id)

        data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'outFields':'OBJECTID'}).encode('utf-8')

        req = urllib.request.Request(FS_service)
        response = urllib.request.urlopen(req,data=data)
        response_payload = response.read()
        response_payload = json.loads(response_payload)
        for oid_item in response_payload['features']:
            objectID = oid_item['attributes']['OBJECTID']
    else:
        objectID = '0'

    if item_count > 0:

        FS_service = service_URL_Area + 'query/?token={}'.format(edit_token)

        where_statement = 'ID=\'{}\''.format(outage_id)

        data = urllib.parse.urlencode({'f':'json', 'where': where_statement, 'outFields':'OBJECTID'}).encode('utf-8')

        req = urllib.request.Request(FS_service)
        response = urllib.request.urlopen(req,data=data)
        response_payload = response.read()
        response_payload = json.loads(response_payload)
        for oid_item_area in response_payload['features']:
            objectID_Area = oid_item_area['attributes']['OBJECTID']
    else:
        objectID_Area = '0'

    return (item_count, objectID, objectID_Area)

def pushToAGOL(outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage, output_starttime,
               output_estrestore, output_impact, output_cause, output_status, output_updatetime, output_long_pt, output_lat_pt, pse_poly):

    # Get token for editing/checking
    edit_token = get_token()

    # Get count
    item_count, objectID, objectID_Area = queryCount(outage_id,edit_token)

    if item_count > 0:
        upload_payload = [{
                'geometry' : {'x' : output_long_pt, 'y' : output_lat_pt},
                'attributes' : {
                    'OBJECTID': objectID,
                    'Title' : outage_title,
                    'Map_Type': outage_mapType,
                    'Pin_Type': outage_pinType,
                    'Planned': output_plannedoutage,
                    'Start': output_starttime,
                    'EstRestore': output_estrestore,
                    'Impact': output_impact,
                    'Cause' : output_cause,
                    'Status' : output_status,
                    'Updated' : output_updatetime
                }
                }]
        upload_payload_Area = [{
                'geometry' : {
                    'rings' : [pse_poly]
                    },
                'attributes' : {
                    'OBJECTID': objectID_Area,
                    'Status' : output_status,
                }
                }]
        update_AGOL(upload_payload, upload_payload_Area, edit_token)
    else:
        upload_payload = [{
                'geometry' : {'x' : output_long_pt, 'y' : output_lat_pt},
                'attributes' : {
                    'ID' : outage_id,
                    'Title' : outage_title,
                    'Map_Type': outage_mapType,
                    'Pin_Type': outage_pinType,
                    'Planned': output_plannedoutage,
                    'Start': output_starttime,
                    'EstRestore': output_estrestore,
                    'Impact': output_impact,
                    'Cause' : output_cause,
                    'Status' : output_status,
                    'Updated' : output_updatetime
                }
                }]
        upload_payload_Area = [{
                'geometry' : {
                    'rings' : [pse_poly]
                    },
                'attributes' : {
                    'ID' : outage_id,
                    'Status' : output_status,
                }
                }]
        insert_AGOL(upload_payload, upload_payload_Area, edit_token)

    return

def insert_AGOL(upload_payload, upload_payload_Area, edit_token):

    FS_service = service_URL + 'addFeatures/?token={}'.format(edit_token)

    data = urllib.parse.urlencode({'f': 'json', 'features': upload_payload}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    the_page = response.read()

    print ('    Point Record inserted.')

    FS_service = service_URL_Area + 'addFeatures/?token={}'.format(edit_token)

    data = urllib.parse.urlencode({'f': 'json', 'features': upload_payload_Area}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    the_page = response.read()

    print ('    Area Record inserted.')

    return

def update_AGOL(upload_payload, upload_payload_Area, edit_token):

    FS_service = service_URL + 'updateFeatures/?token={}'.format(edit_token)

    data = urllib.parse.urlencode({'f': 'json', 'features': upload_payload}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)

    the_page = response.read()

    print ('    Point Record updated.')

    FS_service = service_URL_Area + 'updateFeatures/?token={}'.format(edit_token)

    data = urllib.parse.urlencode({'f': 'json', 'features': upload_payload_Area}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)

    the_page = response.read()

    print ('    Area Record updated.')

    return

def aisle6Cleanup():

    dt = datetime.datetime.utcnow()
    dt = dt - datetime.timedelta(minutes=3)
    dbdt = datetime.datetime.now()

    print ('\n\nCompleting Prior to:  {}'.format(dt))

    edit_token = get_token()

    FS_service = service_URL + 'query/?token={}'.format(edit_token)
    where_statement = 'EditDate<\'{}\' and Status<>\'Complete\''.format(dt)
    data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'outFields':'OBJECTID, ID'}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    response_payload = response.read()
    response_payload = json.loads(response_payload)

    count = 0

    for oid_item in response_payload['features']:

        objectID = oid_item['attributes']['OBJECTID']
        ID = oid_item['attributes']['ID']

        FS_service = service_URL_Area + 'query/?token={}'.format(edit_token)
        where_statement = 'ID=\'{}\''.format(ID)
        data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'outFields':'OBJECTID'}).encode('utf-8')

        req = urllib.request.Request(FS_service)
        response = urllib.request.urlopen(req,data=data)
        response_payload = response.read()
        response_payload = json.loads(response_payload)
        for oid_item in response_payload['features']:
            objectID_Area = oid_item['attributes']['OBJECTID']

        upload_payload = [{
                'attributes' : {
                    'OBJECTID': objectID,
                    'Status' : 'Complete'
                }
                }]

        upload_payload_Area = [{
                'attributes' : {
                    'OBJECTID': objectID_Area,
                    'Status' : 'Complete'
                }
                }]

        update_AGOL(upload_payload, upload_payload_Area, edit_token)
        count += 1

    print ('   {} Records Completed.'.format(count))

    return


#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

getPSE()
aisle6Cleanup()