
#-------------------------------------------------------------------------------
# Name:        PSE Outage Scraper
# Purpose:      This script scrapes the PSE outage data into 2 tables to be used
#               for GIS user needs during contingency/emergency events.
#
# Author:       John Spence
#
# Created:      21 December 2019
# Modified:     07 January 2020
# Copyright:   (c) Spence.dev 2020
# Licence:  GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007
#           https://choosealicense.com/licenses/gpl-3.0/#
#-------------------------------------------------------------------------------

import time
import re
import requests, json, collections, string
import pyodbc

conn_params = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=YOURSERVER;'
                      'Database=YOURDATABASE;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
#                      r'UID=YourUserName;'  # Comment out if you are using AD authentication.
#                      r'PWD=YourPassword'     # Comment out if you are using AD authentication.
                      )

pse_status = 'https://www.pse.com/api/sitecore/OutageMap/AnonymoussMapListView'

status_response = requests.get (pse_status)
status_data = status_response.json()

outage_tracks = []

status_payload = status_data['PseMap']
for item in status_payload:
    ##status_message = item['DataProvider']['Attributes']['Name']
    status_message = item['DataProvider']['Attributes']
    poi_data = item['DataProvider']['PointOfInterest']
    polygon_data = item['Polygon']
    print ('ID: ' + poi_data['Id'])
    outage_id = poi_data['Id']
    outage_tracks.append(outage_id)
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
    		check_field = 0

    if check_field == 0:
    	update_string_choice = 1
    else:
    	update_string_choice = 0

    for message in status_message:
    	print ('	' + message['Name'] + ': ' + message['Value'])
    	if message['Name'] == 'Start time':
    		output_starttime = message['Value']
    	elif message['Name'] == 'Est. restoration time' and update_string_choice == 0:
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
    	print ('	  ' + tp['Longitude'] + ', ' + tp['Latitude'])
    	if init == 0:
            turning_point = tp['Longitude'] + ' ' + tp['Latitude']
            turning_point_S = tp['Longitude'] + ' ' + tp['Latitude']
            init += 1
    	else:
    		turning_point = turning_point + ', ' + tp['Longitude'] + ' ' + tp['Latitude']

    turning_point = turning_point + ', ' + turning_point_S

    payload_search = (('{0}'.format(outage_id)))
    query_string = ("select count(*) from [SAFETY].[PowerOutages] "
    "where ID = '{0}'").format(outage_id)

    query_conn = pyodbc.connect(conn_params)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    rowcount = query_cursor.fetchone()[0]
    result_test = int(rowcount)
##    for row in rows:
##    	print ('Result:  {0}'.format(row[0]))

    if result_test > 0:
        print ('\nOutage Already Exits:  {0}'.format(outage_id))
        print ('	Updating record...\n')

##		if update_string_choice == 1:
##			payload = (outage_title, outage_mapType, outage_pinType, output_plannedoutage,
##			output_starttime, output_impact, output_cause, output_status, output_updatetime,
##			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt, outage_id)
##		else:
##			payload = (outage_title, outage_mapType, outage_pinType, output_plannedoutage,
##			output_starttime, output_estrestore, output_impact, output_cause, output_status, output_updatetime,
##			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt, outage_id)

    	if update_string_choice == 1:
    		update_string = (
    		"update [SAFETY].[PowerOutages] "
    		"Set Title = '{}', Map_Type = '{}', Pin_Type = '{}', Planned = '{}', Start = '{}', Impact = {}, "
    		"Cause = '{}', Status = '{}', Updated = '{}', Longitude = '{}', Latitude = '{}', "
    		"SysChangeDate = current_timestamp, Shape = geometry::STGeomFromText('POINT({} {})', 4326) "
    		"where ID = '{}'").format (outage_title, outage_mapType, outage_pinType, output_plannedoutage,
            output_starttime, output_impact, output_cause, output_status, output_updatetime, output_long_pt,
            output_lat_pt, output_long_pt, output_lat_pt, outage_id)

    	else:
    		update_string = (
    		"update [SAFETY].[PowerOutages] "
    		"Set Title = '{}', Map_Type = '{}', Pin_Type = '{}', Planned = '{}', Start = '{}', EstRestore = '{}', Impact = {}, "
    		"Cause = '{}', Status = '{}', Updated = '{}', Longitude = '{}', Latitude = '{}', "
    		"SysChangeDate = current_timestamp, Shape = geometry::STGeomFromText('POINT({} {})', 4326) "
    		"where ID = '{}'").format (outage_title, outage_mapType, outage_pinType, output_plannedoutage,output_starttime,
            output_estrestore, output_impact, output_cause, output_status, output_updatetime,output_long_pt, output_lat_pt,
            output_long_pt, output_lat_pt, outage_id)

        update_conn = pyodbc.connect(conn_params)
        update_cursor = update_conn.cursor()
        update_cursor.execute(update_string)
        update_conn.commit()

        if result_test > 0:
            Geom_query_string = (
            "select count(*) from [SAFETY].[PowerOutages_Extent] where ID = '{0}' ".format(outage_id))
            #"shape = geometry::STGeomFromText('POLYGON(({1}))', 4326) "
            #"order by SysChangeDate desc ".format(outage_id,turning_point))
            #"order by SysChangeDate desc ".format(outage_id))

            Geom_query_conn = pyodbc.connect(conn_params)
            Geom_query_cursor = query_conn.cursor()
            Geom_query_cursor.execute(Geom_query_string)
            Geom_rows = Geom_query_cursor.fetchone()[0]
            Geom_result_test = int(Geom_rows)

       	    if Geom_result_test > 0:
                print ('	Geometry Already Exits For:  {}'.format(outage_id))
                print ('	No further update required.  Rolling SysChangeDate for last status check.\n\n')
                update_polygon = (
                "update [SAFETY].[PowerOutages_Extent] "
                "Set SysChangeDate = current_timestamp, Shape = geometry::STGeomFromText('POLYGON(({}))', 4326) "
                "where ID = '{}'".format(turning_point, outage_id))
                update_cursor.execute(update_polygon)
                update_conn.commit()

            else:
    			update_polygon = (
    			"insert into [SAFETY].[PowerOutages_Extent] (ID, Shape)"
    			"values ('{}', geometry::STGeomFromText('POLYGON(({}))', 4326))".format(outage_id,turning_point)
    			)
    			update_cursor.execute(update_polygon)
    			update_conn.commit()

        Geom_query_cursor.close()
        Geom_query_conn.close()
        update_cursor.close()
        update_conn.close()

    else:

##		if update_string_choice == 1:
##			payload = (outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
##			output_starttime, output_impact, output_cause, output_status, output_updatetime,
##			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)
##		else:
##			payload = (outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
##			output_starttime, output_estrestore, output_impact, output_cause, output_status, output_updatetime,
##			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)
    	if update_string_choice == 1:
    		update_string = (
    		"insert into [SAFETY].[PowerOutages] (ID, Title, Map_Type, Pin_Type, Planned, Start, Impact, "
    		"Cause, Status, Updated, Longitude, Latitude, Shape)"
    		"values ('{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', geometry::STGeomFromText('POINT({} {})', 4326))").format(
            outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
    		output_starttime, output_impact, output_cause, output_status, output_updatetime,
    		output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)
    	else:
    		update_string = (
    		"insert into [SAFETY].[PowerOutages] (ID, Title, Map_Type, Pin_Type, Planned, Start, EstRestore, Impact, "
    		"Cause, Status, Updated, Longitude, Latitude, Shape)"
    		"values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', geometry::STGeomFromText('POINT({} {})', 4326))").format(
            outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
    		output_starttime, output_estrestore, output_impact, output_cause, output_status, output_updatetime,
    		output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)

    	update_polygon = (
    	"insert into [SAFETY].[PowerOutages_Extent] (ID, Shape)"
    	"values ('{}', geometry::STGeomFromText('POLYGON(({}))', 4326))".format(outage_id,turning_point)
    	)

        update_conn = pyodbc.connect(conn_params)
    	update_cursor = update_conn.cursor()
    	#update_cursor.execute(update_string, payload)
        update_cursor.execute(update_string)
    	update_conn.commit()
    	update_cursor.execute(update_polygon)
    	update_conn.commit()
    	update_cursor.close()
    	update_conn.close()

    query_cursor.close()
    query_conn.close()

	#raw_input("Press Enter to continue...")

# Clear up any old outages.
tracking = 0
for ids in outage_tracks:
    if tracking == 0:
    	avoid_id = '{0}'.format(ids)
    	id_listing = "'{0}'".format(avoid_id)
    	tracking += 1
    else:
    	avoid_id = '{0}'.format(ids)
    	id_listing = id_listing + ", '{0}'".format(avoid_id)
        tracking += 1

cleanup_string = ("update [SAFETY].[PowerOutages] set Status = 'Complete', "
"SysChangeDate = current_timestamp where ID not in ({0})"
" and Status <> 'Complete'".format(id_listing))

finalupdate_conn = pyodbc.connect(conn_params)
finalupdate_cursor = finalupdate_conn.cursor()
finalupdate_cursor.execute(cleanup_string)
finalupdate_conn.commit()
finalupdate_cursor.close()
finalupdate_conn.close()
