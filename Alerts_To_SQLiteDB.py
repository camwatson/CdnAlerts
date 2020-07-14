#imports for threading, logging
import concurrent.futures
import logging
import queue
import threading
import time
import sys

# Imports for processing XML alerts into geoJSON
import xml.etree.cElementTree as et
import pandas as pd
import numpy as np
import sqlite3

# Fetch previous alerts via URL
from urllib.request import Request, urlopen

# Imports for processing streamed data
import re
# Import socket module
import socket     

# Only used for SMS messages
# from twilio.rest import Client

# Background on CAP alerts from Google - https://developers.google.com/public-alerts/reference/cap-google
# Alerts Archive Found Here: https://alertsarchive.pelmorex.com/en.php
# Heartbeat Info also contains the last 10 alerts as references.
# Resources on Alerts - Found here: https://alerts.pelmorex.com/#resources
# Threads/Queue's infomation mostly taken from here: https://realpython.com/intro-to-python-threading/

def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None

def sendtextalert(content):
    """ Send text alert """
    # the following line needs your Twilio Account SID and Auth Token
    # Your Account Sid and Auth Token from twilio.com/user/account
    #$sid = getenv('TWILIO_ACCOUNT_SID');
    #$token = getenv('TWILIO_AUTH_TOKEN');
    #$client = new Client($sid, $token);

    # change the "from_" number to your Twilio number and the "to" number
    # to the phone number you signed up for Twilio with, or upgrade your
    # account to send SMS to any phone number
    #client.messages.create(to="+999999999", 
    #                       from_="+999999999", 
    #                       body=content)
    #client is None
    print (content)

def listener(queue, event, host, port):

    """Listen to socket for emergency alert data"""
    client_socket = socket.socket()

    try:
        client_socket.connect((host, port))
    except:
        print("Connection Error.  Can't connect to: %s", host)
        sys.exit()

    logging.info("Connected.  Awaiting data from: %s", host)
    alldata = ""

    # For reference - https://stackoverflow.com/questions/29692250/restarting-a-thread-in-python
    
    while not event.is_set():
        try: 
            data = client_socket.recv(4096)
            logging.debug("Received data from %s", host)

            #Convert byte like object into a string
            data = data.decode("utf-8")

            alldata = alldata + data
            logging.debug("ALLDATA: %s", alldata)

            # Flag re.S ensures '.' will match anything including newlines
            searchAlert = re.search(r'(<alert.+?>.+?</alert>)(.*)', alldata, re.S|re.I)

            if searchAlert:
                logging.info("Completed alert received via %s.  Sending to queue", host)
                queue.put(searchAlert.group(1))
                logging.info("Queue size is now: %d", queue.qsize())
                # We need a better way to catch more than 1 alert being passed...
                # Still a work in progress for now... just serach again in the remainder... 
                # Flag re.S ensures '.' will match anything including newlines
                searchAlert2 = re.search(r'(<alert.+?>.+?</alert>)(.*)', searchAlert.group(2), re.S|re.I)
                if searchAlert2:
                    logging.info("Another alert found via %s.  Sending to queue", host)
                    queue.put(searchAlert2.group(1))
                    logging.info("Queue size is now: %d", queue.qsize())
                    alldata = searchAlert2.group(2)
                else:
                    #print ("Any extra data:\n", searchAlert.group(2))
                    alldata = searchAlert.group(2)
                searchAlert2 is None

            logging.debug("Pass On Data: %s", alldata)
            searchAlert is None


        except OSError as e:
            # Details on python socket and excetptions: https://docs.python.org/3/library/socket.html
            logging.warning("Listener OSError Issue: %s", e)
            client_socket.close()
            del client_socket
            client_socket = socket.socket()
            try:
                client_socket.connect((host, port))
            except:
                print("Re-Connection Error.  Can't connect to: %s", host)
                sys.exit()
            logging.info("Connection restored.  Awaiting data from: %s", host)
        
        except ConnectionResetError as e:
            logging.warning("Listener ConReset Issue: %s", e)
            client_socket.close()
            del client_socket
            client_socket = socket.socket()
            try:
                client_socket.connect((host, port))
            except:
                print("Re-Connection Error.  Can't connect to: %s", host)
                sys.exit()
            logging.info("Connection restored.  Awaiting data from: %s", host)         
        
        except Exception as e: # work on python 3.x
            logging.warning("Listener ERROR: %s", e)
            

    logging.info("Listener received event. Exiting")
    event.set()
    while not pipeline.empty():
        # Waiting for queue to empty.
        i = 0 
    logging.info("Queue empty.  Shutdown.")
  
def consumer(queue, event):
    """Process message from socket and decide what to do with it."""

    while not event.is_set() or not queue.empty():
        try: 
            message = queue.get()
            searchSender = re.search(r'<sender>(.+?)</sender>', message, re.S|re.I)
            if searchSender:
                logging.debug("Consumer processing queue item (Queue Size=%d)", queue.qsize())
                logging.debug("Sender : %s", searchSender.group(1))

                if searchSender.group(1) != "NAADS-Heartbeat":
                    processalert(message)
                    logging.info("Alert queue item completed.\n")
                else:
                    processhb(message)
                    logging.info("Heartbeat queue item completed.\n")
            else:
                logging.warning("No sender found?")
        except Exception as e: # work on python 3.x
            logging.warning("Consumer ERROR: %s", e)

    logging.info("Consumer received event. Exiting")
    event.set()
    while not pipeline.empty():
        # Waiting for queue to empty.
        i = 0 
    logging.info("Consumer empty.  Shutdown.")

def processhb(hbdata):
    try:
        # Define XML rool DIRECTLY FROM A STRING
        root = et.fromstring(hbdata)

        #Setup namespace for parsing - we need to automate this to the incoming root tag
        # This hard coded namespace works for now, but I could easily see this changing
        ns = {'d': 'urn:oasis:names:tc:emergency:cap:1.2'}
        refs = getvalueofnode(root.find('d:references', ns))
        
        # Create connection to SQLite DB
        conn = sqlite3.connect('cap_data.db')

        # Check to see is any of the previous 10 alerts stored in the HB are missing
        # See appendix 1: Heartbeats here: https://alerts.pelmorex.com/wp-content/uploads/2019/12/NAADS-LMD-User-Guide-R10.0.pdf
        if refs is not None:
            c = conn.cursor()
            for ref in refs.split():
                logging.debug("Is this alert in DB: %s", ref)
                query = "SELECT count(refid) from cap_alerts WHERE refid = '" + ref + "';"
                results = c.execute(query)
                for row in results:
                    # If the count returns zero then we haven't not yet logged this alert
                    # Built URL to retireve it and add it to the queue
                    if row[0] == 0:
                        logging.info("Missing Alert: %s", ref)
                        data = ref.split(',')
                        id = re.sub(r"-","_", data[1])
                        id = re.sub(r"\+","p", id)
                        id = re.sub(r":","_", id)
                        searchsent = re.search(r'(.+?)T.+', data[2], re.I)
                        datepath = searchsent.group(1)
                        datefile = re.sub(r"-","_", data[2])
                        datefile = re.sub(r"\+","p", datefile)
                        datefile = re.sub(r":","_", datefile)
                        url = r"http://capcp1.naad-adna.pelmorex.com/" + datepath + r"/" + datefile + r"I" + id + ".xml"
                        logging.debug("URL: %s", url)
                        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                        output = urlopen(req).read()
                        output = output.decode('utf-8')
                        processalert(output)
            c = None
        logging.info("Heartbeat Processed.")
    except Exception as e:
        logging.warning("ProcessHB ERROR: %s", e)
        logging.warning("Heartbeat processing FAILED.")

def processalert(alertdata):
    """ Process XML alert data and load into SQLite DB """
    try: 
        #parsed_xml = et.parse("canada_alerts.txt")
        #root = parsed_xml.getroot()

        # Define XML rool DIRECTLY FROM A STRING
        root = et.fromstring(alertdata)

        # Start by getting the name space text
        # root.tag
        # ns_text = #need some regex her to use later on.

        #Setup namespace for parsing - we need to automate this to the incoming root tag
        # This hard coded namespace works for now, but I could easily see this changing
        ns = {'d': 'urn:oasis:names:tc:emergency:cap:1.2'}

        # Count how many info sections contain en-CA language
        infos = root.findall("d:info/[d:language='en-CA']", ns)
        logging.debug("English Info Sections: %s", len(infos))

        # Define attribute values at the 'alert' level
        identifier = getvalueofnode(root.find('d:identifier', ns))
        sender = getvalueofnode(root.find('d:sender', ns))
        sent = getvalueofnode(root.find('d:sent', ns))
        status = getvalueofnode(root.find('d:status', ns))
        msgType = getvalueofnode(root.find('d:msgType', ns))
        source = getvalueofnode(root.find('d:source', ns))
        scope = getvalueofnode(root.find('d:scope', ns))
        refs = getvalueofnode(root.find('d:references', ns))
        refID = sender + "," + identifier + "," + sent
        rev_refid = np.nan 

        # Setup schema df schema
        dfacols = ['refID', 'identifier', 'sender', 'sent', 'status', 'msgType', 'source', 'scope', 'refs', 'rev_refid']
        dfa = pd.DataFrame(columns=dfacols)
        dfa = dfa.append(pd.Series([refID, identifier, sender, sent, status, msgType, source, scope, refs, rev_refid], index=dfacols), ignore_index=True)
        
        # Build up the data frames to match the SQL tables... 
        dficols = ['refID', 'infoID', 'language', 'category', 'event', 'responseType', 'urgency', 'severity', 'certainty', 'audience', 'effective', 'expires', 'senderName', 'headline', 'description', 'instruction', 'web']
        dfi = pd.DataFrame(columns=dficols)

        dfarcols = ['refID', 'infoID', 'areaDesc']
        dfar = pd.DataFrame(columns=dfarcols)

        dfpcols = ['areaDesc', 'polygon']
        dfp = pd.DataFrame(columns=dfpcols)

        # Setup a counter for the 'info' items 
        info_count = 0
        sendalert = 0

        # Loop through all the 'info' blocks that have a language degine as en-CA... 
        # we append into the data frames within this loop
        for info_en in root.findall("d:info/[d:language='en-CA']", ns):
            # Define attributes at the 'info' level
            language = getvalueofnode(info_en.find('d:language', ns))
            category = getvalueofnode(info_en.find('d:category', ns))
            event = getvalueofnode(info_en.find('d:event', ns))
            responseType = getvalueofnode(info_en.find('d:responseType', ns))
            urgency = getvalueofnode(info_en.find('d:urgency', ns))
            severity = getvalueofnode(info_en.find('d:severity', ns))
            certainty = getvalueofnode(info_en.find('d:certainty', ns))
            audience = getvalueofnode(info_en.find('d:audience', ns))
            effective = getvalueofnode(info_en.find('d:effective', ns))
            expires = getvalueofnode(info_en.find('d:expires', ns))
            senderName = getvalueofnode(info_en.find('d:senderName', ns))
            headline = getvalueofnode(info_en.find('d:headline', ns))
            description = getvalueofnode(info_en.find('d:description', ns))
            instruction = getvalueofnode(info_en.find('d:instruction', ns))
            web = getvalueofnode(info_en.find('d:web', ns))

            dfi = dfi.append(pd.Series([refID, info_count, language, category, event, responseType, urgency, severity, certainty, audience, effective, expires, senderName, headline, description, instruction, web], index=dficols), ignore_index=True)

            # Loop through each area
            for area in info_en.findall("d:area", ns):
                areaDesc = getvalueofnode(area.find('d:areaDesc', ns))
                logging.debug("Area Impacted: %s", areaDesc)
                # Check for certain areas and whether to send alert - Testing
                if re.search(r'Calgary', areaDesc, re.S|re.I):
                    sendalert = 1
                polygon = getvalueofnode(area.find('d:polygon', ns))
                #print (category, areaDesc, polygon)
                polyWKT = re.sub(r'(-*\d) (-*\d)', r'\1, \2', polygon)
                polyWKT = re.sub(r'(-*\d),(-*\d)', r'\1 \2', polyWKT)
                polyWKT = re.sub(r'(-*\d+\.*\d+) (-*\d+\.*\d+),', r'\2 \1,', polyWKT) # Flip lat/long to long/lat we use WKT format for geom
                polyWKT = re.sub(r'(-*\d+\.*\d+) (-*\d+\.*\d+)$', r'\2 \1', polyWKT) # Flip the last lat/long in list.  Previous sub didn't catch the last one
                polyWKT = "POLYGON ((" + polyWKT + "))"
                #print (polyWKT)
                dfar = dfar.append(pd.Series([refID, info_count, areaDesc], index=dfarcols), ignore_index=True)
                dfp = dfp.append(pd.Series([areaDesc, polyWKT], index=dfpcols), ignore_index=True)


            # Increment the count
            info_count = info_count + 1
        
        # Load data in to SQLite database
        loadalertdb(refID, refs, dfa, dfi, dfar, dfp)

        if sendalert == 1:
            print ("SEND ALERT!")
            sendtextalert(headline)
                  
        logging.info("Alert processed")

    except Exception as e: # work on python 3.x
        logging.info("ProcessAlert ERROR: %s", e)

def loadalertdb(MasterID, refs, alerts, infos, areas, polys):
    try:

        # Create connection to SQLite DB
        conn = sqlite3.connect('cap_data.db')
        c = conn.cursor()
        query = "SELECT count(refid) from cap_alerts WHERE refid = '" + MasterID + "';"
        results = c.execute(query)
        for row in results:
            # If the count returns zero then we haven't not yet logged this alert
            if row[0] == 0:
                logging.info("Loading details on %s into SQLite", MasterID)
                #Load Alert items to table
                alerts.to_sql('cap_alerts', conn, if_exists='append')
                logging.info("Alert data loaded to SQLite.")
                # Load Info items to table
                infos.to_sql('cap_info', conn, if_exists='append')
                logging.info("Info data loaded to SQLite.")
                # Load Area items to table
                areas.to_sql('cap_area', conn, if_exists='append')
                logging.info("Area data loaded to SQLite.")
                
                # Loop through the 'references' for this alert and update the table
                # to indicate that a newer alert now supercedes the old ones        
                if refs is not None:
                    logging.info("Updating referenced alerts...")
                    for prevrefID in refs.split():
                        refquery = "UPDATE cap_alerts SET rev_refid = '" + MasterID + "' WHERE refid = '" + prevrefID + "';"
                        c.execute(refquery)
                        conn.commit()

                # Load polygon areas to table.
                for ind in polys.index:
                    # Replace single quotes with 2 of them to ensure they are escaped for SQL call
                    Area_Desc = polys['areaDesc'][ind].replace("'","''")
                    chkquery = " SELECT count(areaDesc) from cap_poly WHERE areaDesc = '" + Area_Desc + "';"
                    countpoly = c.execute(chkquery)
                    for polyrow in countpoly:
                        # If the count returns zero then we haven't ever loaded this area polygon
                        if polyrow[0] == 0:
                            inquery = "INSERT INTO cap_poly (areaDesc, polygon) VALUES('" + Area_Desc + "', '" + polys['polygon'][ind] + "');"
                            logging.info("New Poly: %s", Area_Desc)
                            c.execute(inquery)
                            conn.commit()
                    #print(polys['areaDesc'][ind], polys['polygon'][ind]) 
                logging.info("Polygon data loaded to SQLite.")
                logging.info("Alert data loaded to SQLite.")
            else:
                logging.info("Alert %s already in database", MasterID)

        c = None
        conn is None

    except Exception as e:
        logging.info("LoadAlertDB ERROR: %s", e)

if __name__ == "__main__":

    primaryhost = "streaming1.naad-adna.pelmorex.com"
    secondaryhost = "streaming2.naad-adna.pelmorex.com"
    hostport = 8080

    try: 
        # Configure logging details
        format = "%(asctime)s.%(msecs)04d: %(message)s"
        logging.basicConfig(filename='C:\\apps\\temp\\alerts.log', format=format, level=logging.INFO,datefmt="%H:%M:%S")
        #logging.basicConfig(format=format, level=logging.INFO,datefmt="%H:%M:%S")
        #logging.getLogger().setLevel(logging.DEBUG)

        # Setup taken from here: https://realpython.com/intro-to-python-threading/
        # Setup threading queue - May need to go larger than 50 if we check for missing data w/ heartbeat
        pipeline = queue.Queue(maxsize=50)
        # Event.set () will stop threads, but will allow them to complete
        event = threading.Event()

        # Setup threadpool with two listener and one consumer (may add a timer later as well)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.submit(listener, pipeline, event, primaryhost, hostport)
            executor.submit(listener, pipeline, event, secondaryhost, hostport)
            executor.submit(consumer, pipeline, event)
            
    except KeyboardInterrupt:
        logging.info("Main: about to set event")
        logging.info("Please wait while the queue empties...")
        event.set()
        while not pipeline.empty():
            # Waiting for queue to empty.
            i = 0 
        print('You cancelled the operation.')
        
    except Exception as e: # work on python 3.x
        print("ERROR: ")
        print(e)
        logging.info("Main: about to set event")
        logging.info("Please wait while the queue empties...")
        event.set()
        while not pipeline.empty():
            # Waiting for queue to empty.
            i = 0 
