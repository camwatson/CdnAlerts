# CdnAlerts
This code listens to TCP-IP streams that contain all emergency alerts in Canada (formatted as CAP alerts).  This data is processed and then loaded into a
SQLlite DB.

1. Background on CAP alerts from Google - https://developers.google.com/public-alerts/reference/cap-google
2. Alerts Archive Found Here: https://alertsarchive.pelmorex.com/en.php
3. A 'heartbeat' is sent every minute.  Heartbeat Info also contains the last 10 alerts as references.
4. Resources on Alerts - Found here: https://alerts.pelmorex.com/#resources
5. Threads/Queue's infomation mostly taken from here: https://realpython.com/intro-to-python-threading/
