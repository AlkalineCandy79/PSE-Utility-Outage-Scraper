#-------------------------------------------------------------------------------
# Name:        WORK IN PROGRESS
# Purpose:
#
# Author:       John Spence
#
# Created:      21 December 2019
# Copyright:   (c) Spence.dev 2019
# Licence:  GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007
#           https://choosealicense.com/licenses/gpl-3.0/#
#-------------------------------------------------------------------------------

import time
import re
import requests, json, collections, string
import mysql.connector

conn_params = {
			'host': 'localhost',
			'port': 3306,
			'database': 'outages',
			'user': 'outage_man',
			'password': '',
			'charset': 'utf8',
			'use_unicode': True,
			'get_warnings': True,
			}


pse_status = 'https://www.pse.com/api/sitecore/OutageMap/AnonymoussMapListView'
outage_details = r'Outage_Tracking'

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
	for tp in polygon_data:
		print ('	  ' + tp['Longitude'] + ', ' + tp['Latitude'])
	print('\n')
	print('\n')
	print('\n')

	payload_search = (('{0}'.format(outage_id)))
	query_string = ("select * from Outage_Tracking "
	"where ID = '{0}'").format(outage_id)

	query_conn = mysql.connector.connect(**conn_params)
	query_cursor = query_conn.cursor()
	query_cursor.execute(query_string)
	rows = query_cursor.fetchall()
	result_test = int(query_cursor.rowcount)
	for row in rows:
		print ('Result:  {0}'.format(row[0]))

	if result_test > 0:
		print ('\nOutage Already Exits:  {0}'.format(outage_id))
		print ('	Updating record...\n')

		if update_string_choice == 1:
			payload = (outage_title, outage_mapType, outage_pinType, output_plannedoutage,
			output_starttime, output_impact, output_cause, output_status, output_updatetime,
			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt, outage_id)
		else:
			payload = (outage_title, outage_mapType, outage_pinType, output_plannedoutage,
			output_starttime, output_estrestore, output_impact, output_cause, output_status, output_updatetime,
			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt, outage_id)

		if update_string_choice == 1:
			update_string = (
			"update Outage_Tracking "
			"Set Title = %s, Map_Type = %s, Pin_Type = %s, Planned = %s, Start = %s, Impact = %s, "
			"Cause = %s, Status = %s, Updated = %s, Longitude = %s, Latitude = %s, "
			"SysChangeDate = current_timestamp(), Shape = ST_GeomFromText('POINT(%s %s)', 4326)"
			"where ID = %s")

		else:

			update_string = (
			"update Outage_Tracking "
			"Set Title = %s, Map_Type = %s, Pin_Type = %s, Planned = %s, Start = %s, EstRestore = %s, Impact = %s, "
			"Cause = %s, Status = %s, Updated = %s, Longitude = %s, Latitude = %s, "
			"SysChangeDate = current_timestamp(), Shape = ST_GeomFromText('POINT(%s %s)', 4326)"
			"where ID = %s")

		update_conn = mysql.connector.connect(**conn_params)
		update_cursor = update_conn.cursor(buffered=True)
		update_cursor.execute(update_string, payload)
		update_conn.commit()
		update_cursor.close()
		update_conn.close()
		print ('	Record update complete!\n')

	else:

		if update_string_choice == 1:
			payload = (outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
			output_starttime, output_impact, output_cause, output_status, output_updatetime,
			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)
		else:
			payload = (outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
			output_starttime, output_estrestore, output_impact, output_cause, output_status, output_updatetime,
			output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)
		if update_string_choice == 1:
			update_string = (
			"insert into Outage_Tracking (ID, Title, Map_Type, Pin_Type, Planned, Start, Impact, "
			"Cause, Status, Updated, Longitude, Latitude, Shape)"
			"values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326))")
		else:
			update_string = (
			"insert into Outage_Tracking (ID, Title, Map_Type, Pin_Type, Planned, Start, EstRestore, Impact, "
			"Cause, Status, Updated, Longitude, Latitude, Shape)"
			"values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326))")

		update_conn = mysql.connector.connect(**conn_params)
		update_cursor = update_conn.cursor()
		update_cursor.execute(update_string, payload)
		update_conn.commit()
		update_cursor.close()
		update_conn.close()

	query_cursor.close()
	query_conn.close()

	##raw_input("Press Enter to continue...")

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

cleanup_string = ("update Outage_Tracking set Status = 'Complete', "
"SysChangeDate = current_timestamp() where ID not in ({0})"
" and Status <> 'Complete'".format(id_listing))

finalupdate_conn = mysql.connector.connect(**conn_params)
finalupdate_cursor = finalupdate_conn.cursor()
finalupdate_cursor.execute(cleanup_string)
finalupdate_conn.commit()
finalupdate_cursor.close()
finalupdate_conn.close()




