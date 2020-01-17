#-------------------------------------------------------------------------------
# Name:        PSE Power Outages Data Extraction
# Purpose:  This script extracts all power outages from the PSE provided online
#           map and applies it to the database.  In the process it creates a shape
#           field in which allows Esri and other spatially enabled products to read
#           and make calculations as such.
#
# Author:      John Spence
#
# Created:      21 December 2019
# Modified:     17 January 2020
# Modification Purpose:  Added in the ability to do db cleanup removing old
#                       outages that may exist past the boundaries of interest.
#                       The query I am using is looking for a polygon intersect.
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

# PyODBC confifguration
conn_params = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=SQL2016STG\STGPROD;'
                      'Database=WebGIS;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
#                      r'UID=YourUserName;'  # Comment out if you are using AD authentication.
#                      r'PWD=YourPassword'     # Comment out if you are using AD authentication.
                      )

# Puget Sound Eneregy Data Source
pse_status = 'https://www.pse.com/api/sitecore/OutageMap/AnonymoussMapListView'

# Set to 1 if you want to cleanup data from outside of your spatial area of interest.
db_cleanup = 1

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import time
import re
import requests, json, collections, string
import pyodbc

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------


## None at this time.  Pending relook at process to breakup functional ops.


#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------


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
    query_string = ("select count(*) from [ITD].[PowerOutages] "
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

    	if update_string_choice == 1:
    		update_string = (
    		"update [ITD].[PowerOutages] "
    		"Set Title = '{}', Map_Type = '{}', Pin_Type = '{}', Planned = '{}', Start = '{}', Impact = {}, "
    		"Cause = '{}', Status = '{}', Updated = '{}', Longitude = '{}', Latitude = '{}', "
    		"SysChangeDate = current_timestamp, Shape = geometry::STGeomFromText('POINT({} {})', 4326) "
    		"where ID = '{}'").format (outage_title, outage_mapType, outage_pinType, output_plannedoutage,
            output_starttime, output_impact, output_cause, output_status, output_updatetime, output_long_pt,
            output_lat_pt, output_long_pt, output_lat_pt, outage_id)

    	else:
    		update_string = (
    		"update [ITD].[PowerOutages] "
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
            "select count(*) from [ITD].[PowerOutages_Extent] where ID = '{0}' ".format(outage_id))

            Geom_query_conn = pyodbc.connect(conn_params)
            Geom_query_cursor = query_conn.cursor()
            Geom_query_cursor.execute(Geom_query_string)
            Geom_rows = Geom_query_cursor.fetchone()[0]
            Geom_result_test = int(Geom_rows)

       	    if Geom_result_test > 0:
                print ('	Geometry Already Exits For:  {}'.format(outage_id))
                print ('	No further update required.  Rolling SysChangeDate for last status check.\n\n')
                update_polygon = (
                "update [ITD].[PowerOutages_Extent] "
                "Set SysChangeDate = current_timestamp, Shape = geometry::STGeomFromText('POLYGON(({}))', 4326) "
                "where ID = '{}'".format(turning_point, outage_id))
                update_cursor.execute(update_polygon)
                update_conn.commit()

            else:
    			update_polygon = (
    			"insert into [ITD].[PowerOutages_Extent] (ID, Shape)"
    			"values ('{}', geometry::STGeomFromText('POLYGON(({}))', 4326))".format(outage_id,turning_point)
    			)
    			update_cursor.execute(update_polygon)
    			update_conn.commit()

        Geom_query_cursor.close()
        Geom_query_conn.close()
        update_cursor.close()
        update_conn.close()

    else:

    	if update_string_choice == 1:
    		update_string = (
    		"insert into [ITD].[PowerOutages] (ID, Title, Map_Type, Pin_Type, Planned, Start, Impact, "
    		"Cause, Status, Updated, Longitude, Latitude, Shape)"
    		"values ('{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', geometry::STGeomFromText('POINT({} {})', 4326))").format(
            outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
    		output_starttime, output_impact, output_cause, output_status, output_updatetime,
    		output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)
    	else:
    		update_string = (
    		"insert into [ITD].[PowerOutages] (ID, Title, Map_Type, Pin_Type, Planned, Start, EstRestore, Impact, "
    		"Cause, Status, Updated, Longitude, Latitude, Shape)"
    		"values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', geometry::STGeomFromText('POINT({} {})', 4326))").format(
            outage_id, outage_title, outage_mapType, outage_pinType, output_plannedoutage,
    		output_starttime, output_estrestore, output_impact, output_cause, output_status, output_updatetime,
    		output_long_pt, output_lat_pt, output_long_pt, output_lat_pt)

    	update_polygon = (
    	"insert into [ITD].[PowerOutages_Extent] (ID, Shape)"
    	"values ('{}', geometry::STGeomFromText('POLYGON(({}))', 4326))".format(outage_id,turning_point)
    	)

        update_conn = pyodbc.connect(conn_params)
    	update_cursor = update_conn.cursor()
        update_cursor.execute(update_string)
    	update_conn.commit()
    	update_cursor.execute(update_polygon)
    	update_conn.commit()
    	update_cursor.close()
    	update_conn.close()

    query_cursor.close()
    query_conn.close()

	#raw_input("Press Enter to continue...")  #Used for debugging.

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

cleanup_string = ("update [ITD].[PowerOutages] set Status = 'Complete', "
"SysChangeDate = current_timestamp where ID not in ({0})"
" and Status <> 'Complete'".format(id_listing))

finalupdate_conn = pyodbc.connect(conn_params)
finalupdate_cursor = finalupdate_conn.cursor()
finalupdate_cursor.execute(cleanup_string)
finalupdate_conn.commit()
finalupdate_cursor.close()
finalupdate_conn.close()

# Purge any ouside of the LIS Map Extent.

if db_cleanup == 1:

    find_cleanupstring = ("select PO.[ID]"
    ", (PE.[Shape].MakeValid()).STIntersects(ME.[Shape]) as OverLap_Test"
    " from [ITD].[PowerOutages] as PO"
    " inner join [ITD].[PowerOutages_Extent] PE on PO.ID = PE.ID"
    " cross join [LIS].[LISMapExtent_WGS84] ME"
    " where PO.Status = 'Complete'"
    " and (PE.[Shape].MakeValid()).STIntersects(ME.[Shape]) = 0")

    query_conn = pyodbc.connect(conn_params)
    query_cursor = query_conn.cursor()
    query_result = query_cursor.execute(find_cleanupstring)

    if result_test > 0:

        print ('\nPurging old outages not part of Bellevue...')

        purge_listing = []

        for ID in query_result:
            item = ID[0]
            purge_listing.append(item)

        query_cursor.close()
        query_conn.close()

        tracking = 0
        for pending in purge_listing:
            if tracking == 0:
            	remove_ID = '{0}'.format(pending)
            	id_listing = "'{0}'".format(remove_ID)
            	tracking += 1
            else:
                remove_ID = '{0}'.format(pending)
                id_listing = id_listing + ", '{0}'".format(remove_ID)
                tracking += 1

        dbpurge_string = ("delete from [ITD].[PowerOutages]"
        "where [ID] in ({})".format(id_listing))
        finalupdate_conn = pyodbc.connect(conn_params)
        finalupdate_cursor = finalupdate_conn.cursor()
        finalupdate_cursor.execute(dbpurge_string)
        finalupdate_conn.commit()
        finalupdate_cursor.close()
        finalupdate_conn.close()

        dbpurge_string = ("delete from [ITD].[PowerOutages_Extent]"
        "where [ID] in ({})".format(id_listing))
        finalupdate_conn = pyodbc.connect(conn_params)
        finalupdate_cursor = finalupdate_conn.cursor()
        finalupdate_cursor.execute(dbpurge_string)
        finalupdate_conn.commit()
        finalupdate_cursor.close()
        finalupdate_conn.close()

        print ('    Purge completed!')

    else:
        query_cursor.close()
        query_conn.close()
        print ('\nNo old outages to purge.')


