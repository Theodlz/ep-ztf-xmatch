import json
import os
import time
from astropy.time import Time

import pandas as pd
from penquins import Kowalski
from db import is_db_initialized, get_db_connection, fetch_events, update_event_status, insert_xmatches, remove_xmatches_by_event_id

FID_TO_BAND = {
    '1': 'g',
    '2': 'r',
    '3': 'i',
}

# DELTA_T = 0.5 # days
DELTA_T = (1 / (60 * 24)) * 20  # 20 minutes in days (JD)
RADIUS_MULTIPLIER = 1.0


BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'
    
def fid_to_band(fid):
    if pd.isna(fid):
        return fid
    return FID_TO_BAND.get(str(int(fid)), pd.NA)

def cone_searches(events: list, k: Kowalski):
    queries = []

    results = {}

    for event in events:
        obs_start = event["obs_start"] # datetime string
        # convert to jd
        jd = Time(obs_start).jd # jd
        
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
                                "candidate.jd": {
                                    "$gte": jd_start,
                                    "$lte": jd_end,
                                },
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
            results[event_name] = matches

    return results

def service(k: Kowalski):
    with get_db_connection() as conn:
        c = conn.cursor()
        events = fetch_events(None, c, status='pending')

        # also query events that should be reprocessed:
        # - the status is done (already queried)
        # - the obs_start is less than 24 hours ago
        # - the last_queried is more than 10 minutes ago
        events_to_reprocess = fetch_events(None, c, can_reprocess=True)
        if not events_to_reprocess:
            events_to_reprocess = []

        events += events_to_reprocess

        if not events:
            return

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
                print(f'Failed to process event {event["name"]}: {e}')
                update_event_status(event['id'], f'failed: {str(e)}', c)
        conn.commit()

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

    while True:
        service(k)
        time.sleep(5)
        print('Service loop')

