from db import is_db_initialized, get_db_connection, fetch_events, fetch_xmatches, set_xmatch_as_processed
from datetime import datetime, timezone, timedelta
import sqlite3
import time
import urllib.parse
import requests
from astropy.time import Time
import os


FRITZ_HOST = os.getenv("FRITZ_HOST")
FRITZ_TOKEN = os.getenv("FRITZ_TOKEN")
FRITZ_FILTER_ID = os.getenv("FRITZ_FILTER_ID")
FRITZ_IMPORT_GROUP_ID = os.getenv("FRITZ_IMPORT_GROUP_ID")
MAX_EVENT_AGE = os.getenv("MAX_EVENT_AGE", 31.0)  # in days, default is 31 days
if FRITZ_HOST is None:
    raise Exception("FRITZ_HOST environment variable is not set.")
if FRITZ_TOKEN is None or FRITZ_TOKEN == "<your-fritz-token>":
    raise Exception("FRITZ_TOKEN environment variable is not set.")
if FRITZ_FILTER_ID is None or FRITZ_FILTER_ID == "<fritz-ztfep-filter-id>":
    raise Exception("FRITZ_FILTER_ID environment variable is not set.")
if FRITZ_IMPORT_GROUP_ID is None or FRITZ_IMPORT_GROUP_ID == "<your-fritz-group-id>":
    raise Exception("FRITZ_IMPORT_GROUP_ID environment variable is not set.")

try:
    FRITZ_FILTER_ID = int(FRITZ_FILTER_ID)
except ValueError:
    raise Exception("FRITZ_FILTER_ID environment variable is not a valid integer.")

try:
    FRITZ_IMPORT_GROUP_ID = int(FRITZ_IMPORT_GROUP_ID)
except ValueError:
    raise Exception("FRITZ_IMPORT_GROUP_ID environment variable is not a valid integer.")
try:
    MAX_EVENT_AGE = float(MAX_EVENT_AGE)
except ValueError:
    raise Exception("MAX_EVENT_AGE environment variable is not a valid float.")


class SkyPortal():
    def __init__(self, host=None, token=None):
        self.host = host
        self.token = token
        # get the ZTF+EP filter id
        self.filter_id, self.group_id = self.get_ztf_ep_filter()

        if self.filter_id is None:
            raise Exception("ZTF+EP filter not found in SkyPortal.")

        print(self)

    def api(
        self, method, endpoint, data=None, params=None, raw_response=False
    ):
        url = urllib.parse.urljoin(self.host, f"/api/{endpoint}")
        headers = {"Authorization": f"token {self.token}"} if self.token else None

        status = 429

        while status in [429, 503]:
            try:
                response = requests.request(method, url, json=data, params=params, headers=headers)
            # catch timeouts
            except requests.exceptions.Timeout:
                print("Request timed out. Waiting for 30 seconds...")
                time.sleep(30)
                continue
            status = response.status_code
            if status == 429:
                print("Rate limit exceeded. Waiting for 1 second...")
                time.sleep(1)
            elif status == 503:
                print("Service unavailable. Waiting for 30 seconds...")
                time.sleep(30)

        if raw_response:
            return response
        else:
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                data = None
            return response.status_code, data
        
    def get_ztf_ep_filter(self):
        # fetch all the filters
        status_code, response = self.api(
            "GET",
            "filters",
        )
        if status_code == 200:
            # find the ZTF filter
            for filter in response["data"]:
                if filter["id"] == FRITZ_FILTER_ID:
                    return filter["id"], filter["group_id"]
        else:
            raise Exception(f"Failed to fetch filters: {response}")
                
        return None, None
    
    def __repr__(self):
        return f"SkyPortal(host={self.host}, group_id={self.group_id}, filter_id={self.filter_id})"
                
    def __str__(self):
        return f"SkyPortal API client for {self.host} (filter_id={self.filter_id}, group_id={self.group_id})"
        
    def post_candidate(
        self,
        alert,
    ):
        
        passed_at_jd = alert.get("jd")
        passed_at = Time(passed_at_jd, format="jd").isot
        
        payload = {
            "id": alert["object_id"],
            "ra": alert.get("ra"),
            "dec": alert.get("dec"),
            "score": alert.get("drb"),
            "filter_ids": [self.filter_id],
            "passing_alert_id": alert["candid"],
            "passed_at": passed_at,
            "origin": "ZTF+EP",
        }

        # Send the POST request to SkyPortal
        status_code, response = sp.api(
            "POST",
            "candidates",
            data=payload,
        )

        if status_code == 200:
            print(f"Candidate {alert['object_id']} posted successfully.")
            return True, False
        
        if isinstance(response, dict) and 'duplicate key value violates unique constraint "candidates_main_index"' in response.get("message", ""):
            print(f"Candidate {alert['object_id']} already exists.")
            return True, True
        
        print(f"Failed to post candidate {alert['object_id']}: {response}")
        return False, False
    
    def import_from_kowalski(self, alert):
        # Fetch the object from Kowalski
        data = {
            'candid': alert['candid'],
            'group_ids': [FRITZ_IMPORT_GROUP_ID],
        }
        status_code, response = self.api(
            "POST",
            f"alerts/{alert['object_id']}",
            data=data,
        )
        if status_code == 200:
            print(f"Fetched object {alert['object_id']} from Kowalski.")
            return True

        print(f"Failed to fetch object {alert['object_id']} from Kowalski: {response}")
        return False
    
    def fetch_annotations(
        self,
        alert,
    ):
        # Fetch the annotations for the object
        status_code, response = self.api(
            "GET",
            f"sources/{alert['object_id']}/annotations",
        )

        if status_code == 200:
            print(f"Fetched annotations for {alert['object_id']}.")
            return response['data']

        print(f"Failed to fetch annotations for {alert['object_id']}: {response}")
        return None
    
    def post_annotations(
        self,
        alert,
        event,
    ):
        # first, fetch existing annotations
        annotations = self.fetch_annotations(alert)
        if annotations is None:
            return False

        # Check if the event is already annotated
        ep_annotations = [
            annotation for annotation in annotations if annotation['origin'] == 'ZTF+EP'
        ]

        ep_mjd = Time(event['obs_start'], format='iso').mjd

        payload = {
            "obj_id": alert["object_id"],
            "origin": f"ZTF+EP",
            "data": {
                'name': [event['name']],
                'delta_t': [round(alert['delta_t'], 2)],
                'distance_arcmin': [round(alert['distance_arcmin'], 2)],
                'drb': [round(alert['drb'], 2)],
                'age': [round(alert['age'], 2)],
                'sgscore': [round(alert['sgscore'], 2) if alert['sgscore'] is not None else None],
                'distpsnr': [round(alert['distpsnr'], 2) if alert['distpsnr'] is not None else None],
                'ssdistnr': [round(alert['ssdistnr'], 2) if alert['ssdistnr'] is not None else None],
                'ssmagnr': [round(alert['ssmagnr'], 2) if alert['ssmagnr'] is not None else None],
                'ndethist': [alert['ndethist']],
                'ep_mjd': [ep_mjd],
            },
            "group_ids": [self.group_id],
        }
        
        if not ep_annotations:
            print(f"Object {alert['object_id']} not annotated yet.")
            # Send the POST request to SkyPortal
            status_code, response = self.api(
                "POST",
                f"sources/{alert['object_id']}/annotations",
                data=payload,
            )
            if status_code == 200:
                print(f"Annotations for {alert['object_id']} posted successfully.")
                return True

            print(f"Failed to post annotations for {alert['object_id']}: {response}")
            return False
        
        ep_annotation = ep_annotations[0]
        total = len(ep_annotation["data"]["name"])
        # from the existing annotation, recreate the list of annotated events:
        annoted_events = [
            {
                "name": ep_annotation["data"].get("name", [None] * total)[i],
                "delta_t": ep_annotation["data"].get("delta_t", [None] * total)[i],
                "distance_arcmin": ep_annotation["data"].get("distance_arcmin", [None] * total)[i],
                "drb": ep_annotation["data"].get("drb", [None] * total)[i],
                "age": ep_annotation["data"].get("age", [None] * total)[i],
                "sgscore": ep_annotation["data"].get("sgscore", [None] * total)[i],
                "distpsnr": ep_annotation["data"].get("distpsnr", [None] * total)[i],
                "ssdistnr": ep_annotation["data"].get("ssdistnr", [None] * total)[i],
                "ssmagnr": ep_annotation["data"].get("ssmagnr", [None] * total)[i],
                "ndethist": ep_annotation["data"].get("ndethist", [None] * total)[i],
                "ep_mjd": ep_annotation["data"].get("ep_mjd", [None] * total)[i],
            }
            for i in range(total)
        ]

        # Check if the event of the current candidate has already been annotated already
        found = False
        for event_annotated in annoted_events:
            if event_annotated["name"] == event["name"]:
                # if so, update it's data
                event_annotated["delta_t"] = round(alert["delta_t"], 2)
                event_annotated["distance_arcmin"] = round(alert["distance_arcmin"], 2)
                event_annotated["drb"] = round(alert["drb"], 2)
                event_annotated["age"] = round(alert["age"], 2)
                event_annotated["sgscore"] = round(alert["sgscore"], 2) if alert['sgscore'] is not None else None
                event_annotated["distpsnr"] = round(alert["distpsnr"], 2) if alert['distpsnr'] is not None else None
                event_annotated["ssdistnr"] = round(alert["ssdistnr"], 2) if alert['ssdistnr'] is not None else None
                event_annotated["ssmagnr"] = round(alert["ssmagnr"], 2) if alert['ssmagnr'] is not None else None
                event_annotated["ndethist"] = alert['ndethist']
                event_annotated["ep_mjd"] = ep_mjd
                found = True
                break

        # if not, append it to the list
        if not found:
            annoted_events.append({
                "name": event["name"],
                "delta_t": round(alert["delta_t"], 2),
                "distance_arcmin": round(alert["distance_arcmin"], 2),
                "drb": round(alert["drb"], 2),
                "age": round(alert["age"], 2),
                "sgscore": round(alert["sgscore"], 2) if alert['sgscore'] is not None else None,
                "distpsnr": round(alert["distpsnr"], 2) if alert['distpsnr'] is not None else None,
                "ssdistnr": round(alert["ssdistnr"], 2) if alert['ssdistnr'] is not None else None,
                "ssmagnr": round(alert["ssmagnr"], 2) if alert['ssmagnr'] is not None else None,
                "ndethist": alert['ndethist'],
                "ep_mjd": ep_mjd,
            })

        # Update the payload with the new list of annotated events
        payload["data"]["name"] = [event_annotated["name"] for event_annotated in annoted_events]
        payload["data"]["delta_t"] = [event_annotated["delta_t"] for event_annotated in annoted_events]
        payload["data"]["distance_arcmin"] = [event_annotated["distance_arcmin"] for event_annotated in annoted_events]
        payload["data"]["drb"] = [event_annotated["drb"] for event_annotated in annoted_events]
        payload["data"]["age"] = [event_annotated["age"] for event_annotated in annoted_events]
        payload["data"]["sgscore"] = [event_annotated["sgscore"] for event_annotated in annoted_events]
        payload["data"]["distpsnr"] = [event_annotated["distpsnr"] for event_annotated in annoted_events]
        payload["data"]["ssdistnr"] = [event_annotated["ssdistnr"] for event_annotated in annoted_events]
        payload["data"]["ssmagnr"] = [event_annotated["ssmagnr"] for event_annotated in annoted_events]
        payload["data"]["ndethist"] = [event_annotated["ndethist"] for event_annotated in annoted_events]
        payload["data"]["ep_mjd"] = [event_annotated["ep_mjd"] for event_annotated in annoted_events]

        # add the author_id of the existing annotation
        payload["author_id"] = ep_annotation["author_id"]

        # Send the PUT request to SkyPortal
        status_code, response = self.api(
            "PUT",
            f"sources/{alert['object_id']}/annotations/{ep_annotation['id']}",
            data=payload,
        )
        if status_code == 200:
            print(f"Annotations for {alert['object_id']} updated successfully.")
            return True

        print(f"Failed to update annotations for {alert['object_id']}: {response}")
        return False

def process_xmatch(xmatch, c: sqlite3.Cursor):

    # 1. Grab the event for that match
    events, count = fetch_events(
        event_names=None,
        event_ids=[xmatch["event_id"]],
        c=c,
    )
    if count == 0:
        print(f"Failed to find event {xmatch['event_id']} for xmatch {xmatch['object_id']} (candid {xmatch['candid']}).")
        return False, False
    event = events[0]

    # Check if the event is older than X days
    obs_after = datetime.now(timezone.utc) - timedelta(hours=MAX_EVENT_AGE * 24)
    event_time = datetime.strptime(event["obs_start"], "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc)
    if event_time < obs_after:
        print(f"Event {event['name']} associated to xmatch {xmatch['object_id']} (candid {xmatch['candid']}) is older than 24 hours. Skipping.")
        return True, True

    # 2. Post the candidate to SkyPortal
    posted, already_posted = sp.post_candidate(xmatch)
    if not posted:
        print(f"Failed to post candidate {xmatch['object_id']}.")
        return False, False
    
    # Check if we have a candidate with the same object_id 
    # but a higher JD that was already posted
    newer_xmatches_processed_count = c.execute(
        """
        SELECT COUNT(*) FROM xmatches
        WHERE object_id = ?
        AND jd > ?
        AND to_skyportal = 1
        """,
        (xmatch["object_id"], xmatch["jd"]),
    ).fetchone()['COUNT(*)']
    
    # 3. Import the object's data (phot + cutouts) from Kowalski
    #    Only do so if the candid wasn't already posted to SkyPortal
    #    or if no newer candidates with the same object_id were already posted
    if not already_posted and newer_xmatches_processed_count == 0:
        imported = sp.import_from_kowalski(xmatch)
        if not imported:
            print(f"Failed to import object {xmatch['object_id']} from Kowalski.")
            return False, False

    # 4. Post the annotations
    posted = sp.post_annotations(xmatch, event)
    if not posted:
        print(f"Failed to post/update annotations for {xmatch['object_id']}.")
        return False, False
    
    print(f"Processed xmatch {xmatch['object_id']} successfully.")
    return True, False

if __name__ == "__main__":
    # Check if the database is initialized
    if not is_db_initialized():
        print("Database is not initialized. Please run the migration script first.")
        exit(1)

    try:
        sp = SkyPortal(
            host=FRITZ_HOST,
            token=FRITZ_TOKEN,
        )
    except Exception as e:
        print(f"Failed to initialize SkyPortal: {e}")
        exit(1)

    while True:
        with get_db_connection() as conn:
            # Fetch events and xmatches from the database

            # Only process xmatches that were created in the last 24 hours
            created_after = datetime.now(timezone.utc) - timedelta(days=1)

            # Only process candidates that are less than 2 months old
            detected_after = float(Time(
                datetime.now(timezone.utc) - timedelta(days=62)
            ).jd)

            xmatches, count = fetch_xmatches(
                event_ids=None,
                c=conn,
                to_skyportal=False,
                created_after=created_after,
                detected_after=detected_after,
                eventAgeDays=MAX_EVENT_AGE,
            )

            print(f"Found {count} xmatches to process.")

            # find the number of unique candid and unique objectid, just for logging
            unique_candid = len(set([xmatch["candid"] for xmatch in xmatches]))
            unique_objectid = len(set([xmatch["object_id"] for xmatch in xmatches]))
            print(f"Found {unique_candid} unique candidates and {unique_objectid} unique object ids.")

            for xmatch in xmatches:
                try:
                    processed, skipped = process_xmatch(xmatch, conn)
                    if processed and skipped:
                        continue
                    if processed:
                        set_xmatch_as_processed(xmatch["id"], conn)
                        conn.commit()
                    time.sleep(5)
                except Exception as e:
                    print(f"Error processing xmatch {xmatch['object_id']}: {e}")
                    continue

        print("All xmatches processed, sleeping for 1 minute.")
        time.sleep(60)

