from db import is_db_initialized, get_db_connection, fetch_events, fetch_xmatches, set_xmatch_as_processed
from datetime import datetime, timezone, timedelta
import time
import urllib.parse
import requests
from astropy.time import Time
import os


FRITZ_HOST = os.getenv("FRITZ_HOST")
FRITZ_TOKEN = os.getenv("FRITZ_TOKEN")
FRITZ_FILTER_ID = os.getenv("FRITZ_FILTER_ID")
if FRITZ_HOST is None:
    raise Exception("FRITZ_HOST environment variable is not set.")
if FRITZ_TOKEN is None or FRITZ_TOKEN == "<your-fritz-token>":
    raise Exception("FRITZ_TOKEN environment variable is not set.")
if FRITZ_FILTER_ID is None or FRITZ_FILTER_ID == "<fritz-ztfep-filter-id>":
    raise Exception("FRITZ_FILTER_ID environment variable is not set.")


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
        print(f"Passed at: {passed_at}")
        
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
            return True
        
        if isinstance(response, dict) and 'duplicate key value violates unique constraint "candidates_main_index"' in response.get("message", ""):
            print(f"Candidate {alert['object_id']} already exists.")
            return True
        
        print(f"Failed to post candidate {alert['object_id']}: {response.text}")
        return False
    
    def import_from_kowalski(self, alert):
        # Fetch the object from Kowalski
        status_code, response = self.api(
            "POST",
            f"alerts/{alert['object_id']}?candid={alert['candid']}",
        )
        if status_code == 200:
            print(f"Fetched object {alert['object_id']} from Kowalski.")
            return True

        print(f"Failed to fetch object {alert['object_id']} from Kowalski: {response.text}")
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

        print(f"Failed to fetch annotations for {alert['object_id']}: {response.text}")
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

        payload = {
            "obj_id": alert["object_id"],
            "origin": f"ZTF+EP",
            "data": {
                'name': [event['name']],
                'delta_t': [round(alert['delta_t'], 2)],
                'distance_arcmin': [round(alert['distance_arcmin'], 2)],
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

            print(f"Failed to post annotations for {alert['object_id']}: {response.text}")
            return False
        
        ep_annotation = ep_annotations[0]
        # from the existing annotation, recreate the list of annotated events:
        annoted_events = [
            {
                "name": ep_annotation["data"]["name"][i],
                "delta_t": ep_annotation["data"]["delta_t"][i],
                "distance_arcmin": ep_annotation["data"]["distance_arcmin"][i],
            }
            for i in range(len(ep_annotation["data"]["name"]))
        ]

        # Check if the event of the current candidate has already been annotated already
        found = False
        for event_annotated in annoted_events:
            if event_annotated["name"] == event["name"]:
                # if so, update it's data
                event_annotated["delta_t"] = round(alert["delta_t"], 2)
                event_annotated["distance_arcmin"] = round(alert["distance_arcmin"], 2)
                found = True
                break

        # if not, append it to the list
        if not found:
            annoted_events.append({
                "name": event["name"],
                "delta_t": round(alert["delta_t"], 2),
                "distance_arcmin": round(alert["distance_arcmin"], 2),
            })

        # Update the payload with the new list of annotated events
        payload["data"]["name"] = [event_annotated["name"] for event_annotated in annoted_events]
        payload["data"]["delta_t"] = [event_annotated["delta_t"] for event_annotated in annoted_events]
        payload["data"]["distance_arcmin"] = [event_annotated["distance_arcmin"] for event_annotated in annoted_events]

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

        print(f"Failed to update annotations for {alert['object_id']}: {response.text}")
        return False

def process_xmatch(xmatch, c):
    # 1. Post the candidate to SkyPortal
    posted = sp.post_candidate(xmatch)
    if not posted:
        print(f"Failed to post candidate {xmatch['object_id']}.")
        return False
    
    # 2. Import the object's data (phot + cutouts) from Kowalski
    imported = sp.import_from_kowalski(xmatch)
    if not imported:
        print(f"Failed to import object {xmatch['object_id']} from Kowalski.")
        return False

    # 3. Grab the event for that match
    events, count = fetch_events(
        event_names=None,
        event_ids=[xmatch["event_id"]],
        c=conn,
    )
    if count == 0:
        print(f"Failed to find event {xmatch['event_id']} for xmatch {xmatch['object_id']} (candid {xmatch['candid']}).")
        return False
    event = events[0]

    # 3. Post the annotations
    posted = sp.post_annotations(xmatch, event)
    if not posted:
        print(f"Failed to post/update annotations for {xmatch['object_id']}.")
        return False
    
    print(f"Processed xmatch {xmatch['object_id']} successfully.")
    return True

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

    # Only process xmatches that were created in the last 24 hours
    created_after = datetime.now(timezone.utc) - timedelta(days=1)

    with get_db_connection() as conn:
        # Fetch events and xmatches from the database
        xmatches, count = fetch_xmatches(
            event_ids=None,
            c=conn,
            to_skyportal=False,
            # only get xmatches that were created
            # after the last 24 hours
            created_after=created_after,
        )

        print(f"Found {count} xmatches to process.")

        for xmatch in xmatches:
            try:
                process_xmatch(xmatch, conn)
                set_xmatch_as_processed(xmatch["id"], conn)
                time.sleep(5)
            except Exception as e:
                print(f"Error processing xmatch {xmatch['object_id']}: {e}")
                continue

