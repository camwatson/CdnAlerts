# Canada Emergency Alerts Via Pelmorex TCP/IP Streams

As part of Canada's emergency alert system Pelmorex’s NAAD System collects public safety messages from authorized government authorities and distributes those messages by satellite and through the internet to broadcasting undertakings such as radio and television stations, cable and satellite TV companies, etc. This broadcast is available publically.  This python code consumes the TCP/IP streams broadcast by Pelmorex and captures the data into a simple SQLLite database.

## Main Script: Alerts_To_SQLiteDB.py
* Required libraries: pandas and sqlite3
* Optional: geopandas, shapely and twilio (these libraries are used to spatially locate alerts and send a text message if it intersects a certain area)

This script currently aggresively logs all sorts of information as it is very much a work in progress.  Be warned that the log file can easily exceed 1 Mb in size every 24 hours.

The database is provided with example data (cap_alerts.db) and is fairly simple:
* cap_alert - Each alert sent contains a sender, reference ID and some other basic information.
* cap_info - Each alert may contain multiple information blocks (this is exclusive to Envrionment Canada alerts)
* cap_area - Each information block may reference multiple areas
* cap_poly - Each area is defined as a polygon and are unique and only stored once.

I've utilized ThreadPoolExecuter to listen to multiple tcp/ip streams and push them via a pipeline (in a producer and the consumer setup).  The basic structure of the application is as follows:
* Using a ThreadPoolExecuter we generate 2 threads listening to redundant tcp/ip streams and 1 thread that is a consumer
* As the listener receives data it decodes it utilizing utf-8, and then continues to compile data until it there is a complete xml alert (i.e. - \<alert> \</alert>)
* Via a pipeline the listener adds the alert to a queue
* The consumer thread picks up items as they added to the queue are identifes if it appears to be a real alert or a 'heartbeat'
* If a heatbeat is identified the xml data contains the last 10 alerts.  We poll the database to see if those alerts have been loaded into the database.  If they haven't we query them from the Pelmorex archive feed and load them into the database
* If an actual alert is idenfied we breakdown the information in the xml structure, check to see if the alert has been previously loaded into the database (with redudant tcp/ip streams every alert will be duplicated in the process, but we only store one)
* If the alerts hasn't already been added to the database we load the appropriate data into the 3 tables
* For the cap_poly table we check to see if the polygon has been loaded already.  If not we load the polygon data as WKT into the table
* Once the data is loaded into the database we check to see if the newly added alert intersects areas of interest (maps.geojson - deliberatly excluded.  Other users will have to add their own file and it must contain an attribute called 'Name' - see http://geojson.io/ to create a file)
* If the data intersects the areas of interest we send a text message with Twilio (users will need to create a Twilio account and create envrionment variables with token, id, to and from numbers) including alert details.

1. Background on CAP alerts from Google - https://developers.google.com/public-alerts/reference/cap-google
2. Alerts Archive Found Here: https://alertsarchive.pelmorex.com/en.php
3. A 'heartbeat' is sent every minute.  Heartbeat Info also contains the last 10 alerts as references.
4. Resources on Alerts - Found here: https://alerts.pelmorex.com/#resources
5. Threads/Queue's infomation mostly taken from here: https://realpython.com/intro-to-python-threading/
