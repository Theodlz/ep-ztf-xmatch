import os
import time
from astropy.time import Time
import traceback
import requests
import numpy as np

from penquins import Kowalski
from db import is_db_initialized, get_db_connection, fetch_events, update_event_status, insert_xmatches, remove_xmatches_by_event_id, insert_events, ALLOWED_EVENT_COLUMNS

DELTA_T_DEFAULT = 1.0  # 1 JD by default
RADIUS_MULTIPLIER_DEFAULT = 1.0
DELTA_T = float(os.getenv('DELTA_T', DELTA_T_DEFAULT))
RADIUS_MULTIPLIER = float(os.getenv('RADIUS_MULTIPLIER', RADIUS_MULTIPLIER_DEFAULT))

EP_BASE_URL = "https://ep.bao.ac.cn/ep"
EP_TOKEN_URL = f"{EP_BASE_URL}/api/get_tokenp"
EP_EVENTS_URL = f"{EP_BASE_URL}/data_center/api/unverified_candidates"

EP_EMAIL = os.getenv('EP_EMAIL')
EP_PASSWORD = os.getenv('EP_PASSWORD')

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'

def great_circle_distance(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    """
        Distance between two points on the sphere
    :param ra1_deg:
    :param dec1_deg:
    :param ra2_deg:
    :param dec2_deg:
    :return: distance in degrees
    """
    # this is orders of magnitude faster than astropy.coordinates.Skycoord.separation
    DEGRA = np.pi / 180.0
    ra1, dec1, ra2, dec2 = (
        ra1_deg * DEGRA,
        dec1_deg * DEGRA,
        ra2_deg * DEGRA,
        dec2_deg * DEGRA,
    )
    delta_ra = np.abs(ra2 - ra1)
    distance = np.arctan2(
        np.sqrt(
            (np.cos(dec2) * np.sin(delta_ra)) ** 2
            + (
                np.cos(dec1) * np.sin(dec2)
                - np.sin(dec1) * np.cos(dec2) * np.cos(delta_ra)
            )
            ** 2
        ),
        np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(delta_ra),
    )

    return distance * 180.0 / np.pi

def cone_searches(events: list, k: Kowalski):
    queries = []

    results = {}

    for event in events:
        obs_start = event["obs_start"] # datetime string
        # convert to jd
        jd = Time(obs_start).jd # jd

        # we get the midpoint of the 30 minute window
        jd = jd + (15 / (60 * 24))
        
        jd_start = jd - DELTA_T
        jd_end = jd + DELTA_T
        radius = event["pos_err"] * 60 * 60 # degrees to arcsec
        queries.append(
            {
                "query_type": "cone_search",
                "query": {
                    "object_coordinates": {
                        "radec": {
                            str(event["name"]): [
                                event["ra"],
                                event["dec"]
                            ]
                        },
                        "cone_search_radius": radius * RADIUS_MULTIPLIER,
                        "cone_search_unit": "arcsec",
                    },
                    "catalogs": {
                        "ZTF_alerts": {
                            "filter": {
                                "candidate.jd": { # only consider alerts a the time window of the event
                                    "$gte": jd_start,
                                    "$lte": jd_end,
                                },
                                "candidate.rb": {
                                    "$gt": 0.3 # remove bogus detections (random forest)
                                },
                                "candidate.drb": {
                                    "$gt": 0.5 # remove bogus detections (deep learning)
                                },
                                "candidate.jdstarthist": { # remove old objects
                                    "$gte": jd_start - 1,
                                },
                                "candidate.isdiffpos": { "$in": ["t", "T", "true", "True", True, "1", 1] },
                                "$and": [
                                    { # remove known stellar sources
                                        "$or": [
                                            {"candidate.sgscore1": {"$lt": 0.7}},
                                            {"candidate.distpsnr1": {"$gt": 10}},
                                            {"candidate.distpsnr2": {"$lt": 0}},
                                            {"candidate.distpsnr2": {"$eq": None}},
                                        ]
                                    },
                                    { # remove known solar system objects
                                        "$or": [
                                            {
                                            "candidate.ssdistnr": {
                                                "$lt": 0
                                            }
                                            },
                                            {
                                            "candidate.ssdistnr": {
                                                "$gte": 12
                                            }
                                            },
                                            {
                                            "candidate.ssmagnr": {
                                                "$lte": -20
                                            }
                                            },
                                            {
                                            "candidate.ssmagnr": {
                                                "$gte": 20
                                            }
                                            }
                                        ]
                                    }
                                ]
                            },
                            "projection": {
                                "_id": 0,
                                "candid": 1,
                                "object_id": "$objectId",
                                "jd": "$candidate.jd",
                                "ra": "$candidate.ra",
                                "dec": "$candidate.dec",
                                "fid": "$candidate.fid",
                                "magpsf": "$candidate.magpsf",
                                "sigmapsf": "$candidate.sigmapsf",
                                "drb": "$candidate.drb",
                            }
                        }
                    }
                },
            }
        )

    # now we submit the queries in parallel, with up to 8 threads
    responses = k.query(queries=queries, use_batch_query=True, max_n_threads=4)

    results = {
        event["name"]: [] for event in events
    }

    for response in responses.get('default', []):
        if response.get('status') != 'success':
            print(f'Failed to get objects at positions: {response.get("message", "")}')
            continue

        for event_name, matches in response.get('data', {}).get('ZTF_alerts', {}).items():
            # to each matches, add a delta_t field
            # and the distance to the event position in arcsec
            for match in matches:
                match['delta_t'] = match['jd'] - jd
                match['distance_arcmin'] = great_circle_distance(
                    event['ra'], event['dec'], match['ra'], match['dec']
                ) * 60
                # and a distance_arcmin / pos_err ratio
                match['distance_ratio'] = match['distance_arcmin'] / (event['pos_err'] * 60)

            results[event_name] = matches

    return results

def get_ep_token():
    if EP_EMAIL is None or EP_PASSWORD is None:
        raise ValueError('EP_USERNAME or EP_PASSWORD not set')
    response = requests.post(
        url=EP_TOKEN_URL,
        json={"email": EP_EMAIL, "password": EP_PASSWORD},
        headers={"Content-Type": "application/json"}
    )
    response.raise_for_status()  
    token = response.json().get("token") 
    return token

def get_new_events():
    token = get_ep_token()

    response = requests.get(
                url=EP_EVENTS_URL,
                headers={"tdic-token": token},  
                params={"token": token}
            )
    response.raise_for_status()
    try:
        events = response.json()
    except ValueError:
        events = []
    return events

def service(k: Kowalski, last_event_fetch: float) -> float:
    with get_db_connection() as conn:
        c = conn.cursor()

        # GET NEW EP EVENTS
        if (
            last_event_fetch is None or
            time.time() - last_event_fetch > 5 * 60
        ):
            print('Fetching new events...')
            try:
                new_events = get_new_events()
                last_event_fetch = time.time()
            except Exception as e:
                traceback.print_exc()
                print(f'Failed to get new events: {e}')
                new_events = []
            # check that they each have the allowed columns we need
            for event in new_events:
                missing = [col for col in ALLOWED_EVENT_COLUMNS if col not in event]
                if missing:
                    print(event)
                    raise ValueError('Event does not have all required columns: ' + ', '.join(missing))
                    
            if len(new_events) > 0:
                print(f'Inserting {len(new_events)} events (including duplicates)')
                try:
                    insert_events(new_events, c)
                    conn.commit()
                except Exception as e:
                    traceback.print_exc()
                    print(f'Failed to insert events: {e}')
            
        # QUERY SOURCES FOR NEW EVENTS
        events, _ = fetch_events(None, c, status='pending')

        # also query events that should be reprocessed:
        # - the status is done (already queried)
        # - the obs_start is less than 24 hours ago
        # - the last_queried is more than 10 minutes ago
        events_to_reprocess, _ = fetch_events(None, c, can_reprocess=True)
        if not events_to_reprocess:
            events_to_reprocess = []

        events += events_to_reprocess

        if not events:
            return last_event_fetch

        print(f'Found {len(events)} events to process (including {len(events_to_reprocess)} to reprocess)')

        for event in events:
            try:
                if event['query_status'] == 'pending':
                    update_event_status(event['id'], 'processing', c)

                results = cone_searches([event], k)
                print(f'Found {len(results[event["name"]])} matches for event {event["name"]}')

                xmatches = results[event["name"]]
                if len(xmatches) > 0:
                    for xmatch in xmatches:
                        xmatch['event_id'] = event['id']
                    remove_xmatches_by_event_id(event['id'], c)
                    insert_xmatches(xmatches, c)

                update_event_status(event['id'], 'done', c)
            except Exception as e:
                traceback.print_exc()
                print(f'Failed to process event {event["name"]}: {e}')
                update_event_status(event['id'], f'failed: {str(e)}', c)
        conn.commit()

    return last_event_fetch

if __name__ == "__main__":
    protocol = 'https'
    host = 'kowalski.caltech.edu'
    port = 443
    timeout = 10
    token = os.getenv('KOWALSKI_TOKEN')

    k = Kowalski(
        protocol=protocol,
        host=host,
        port=port,
        token=token,
        timeout=timeout,
    )

    while not is_db_initialized():
        print('Waiting for database to be initialized...')
        time.sleep(15)

    last_event_fetch = None

    while True:
        try:
            last_event_fetch = service(k, last_event_fetch)
        except Exception as e:
            traceback.print_exc()
            print(f'Failed to run service: {e}')
        time.sleep(5)
        print('Service loop')

