# Canada Emergency Alerts Via Pelmorex TCP/IP Streams

As part of Canada's emergency alert system Pelmorex’s NAAD System collects public safety messages from authorized government authorities and distributes those messages by satellite and through the internet to broadcasting undertakings such as radio and television stations, cable and satellite TV companies, etc. This broadcast is available publically.  This python code consumes the TCP/IP streams broadcast by Pelmorex and captures the data into a simple SQLLite database.  

Required libraries: pandas and sqlite3
Optional: geopandas, shapely and twilio (these libraries are used to spatially locate alerts and send a text message if it intersects a certain area)

The database is provided with example data (cap_alerts.db) and is fairly simple:
cap_alert - Each alert sent contains a sender, reference ID and some other basic information.
cap_info - Each alert may contain multiple information blocks (this is exclusive to Envrionment Canada alerts)
cap_area - Each information block may refernce multiple areas
cap_poly - Each area is defined as a polygon and are unique and only stored once.

I've utilized ThreadPoolExecuter to listen to multiple tcp/ip streams and push them pipeline (in a producer and the consumer setup)

1. Background on CAP alerts from Google - https://developers.google.com/public-alerts/reference/cap-google
2. Alerts Archive Found Here: https://alertsarchive.pelmorex.com/en.php
3. A 'heartbeat' is sent every minute.  Heartbeat Info also contains the last 10 alerts as references.
4. Resources on Alerts - Found here: https://alerts.pelmorex.com/#resources
5. Threads/Queue's infomation mostly taken from here: https://realpython.com/intro-to-python-threading/
