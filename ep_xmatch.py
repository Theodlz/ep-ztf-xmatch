import os
import time
from astropy.time import Time
import traceback
import numpy as np

from penquins import Kowalski
from db import is_db_initialized, get_db_connection, fetch_events, update_event_status, insert_xmatches

RADIUS_MULTIPLIER_DEFAULT = 1.0
RADIUS_MULTIPLIER = float(os.getenv('RADIUS_MULTIPLIER', RADIUS_MULTIPLIER_DEFAULT))

DELTA_T_DEFAULT = 1.0  # 1 JD by default
DELTA_T = float(os.getenv('DELTA_T', DELTA_T_DEFAULT))

DELTA_T_ARCHIVAL = 31.0 # 31 JD (a month) by default
DELTA_T_ARCHIVAL = float(os.getenv('DELTA_T_ARCHIVAL', DELTA_T_ARCHIVAL))

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

def is_red_star(match):
    sgscore = match.get('sgscore', -999)
    distpsnr = match.get('distpsnr', -999)
    srmag = match.get('srmag', -999)
    simag = match.get('simag', -999)
    szmag = match.get('szmag', -999)

    if (
        distpsnr < 0 or distpsnr > 1.0
        or sgscore <= 0.2
    ):
        return False
    
    if (
        srmag > 0 and simag > 0
        and srmag - simag > 3
    ):
        return True
    
    if (
        srmag > 0 and szmag > 0
        and srmag - szmag > 3
    ):
        return True
    
    if (
        simag > 0 and szmag > 0
        and simag - szmag > 3
    ):
        return True
    
    return False

def cone_searches(events: list, k: Kowalski, archival: bool = False):
    queries = []

    results = {}

    for event in events:
        obs_start = event["obs_start"] # datetime string
        # convert to jd
        jd = Time(obs_start).jd # jd
        
        if archival:
            # for archival searches, look for candidates BEFORE the event time
            jd_start = jd - DELTA_T - DELTA_T_ARCHIVAL
            jd_end = jd - DELTA_T
        else:
            # for normal searches, look for candidates after the event time
            jd_start = jd - DELTA_T
            jd_end = jd + DELTA_T_ARCHIVAL 
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
                                "candidate.isdiffpos": {
                                    "$in": ["t", "T", "true", "True", True, "1", 1]
                                },
                                "$and": [
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
                                    },
                                    { # remove known stars based on sgscore and associated distance
                                        "$or": [
                                            {
                                                "candidate.sgscore1": {
                                                    "$lt": 0.7
                                                }
                                            },
                                            {
                                                "candidate.distpsnr1": {
                                                    "$gt": 2
                                                }
                                            },
                                            {
                                                "candidate.distpsnr1": {
                                                    "$lt": 0
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
                                "jdstarthist": "$candidate.jdstarthist",
                                "sgscore": "$candidate.sgscore1",
                                "distpsnr": "$candidate.distpsnr1",
                                "ssdistnr": "$candidate.ssdistnr",
                                "ssmagnr": "$candidate.ssmagnr",
                                "ndethist": "$candidate.ndethist",
                                # we grab some extra fields to remove
                                # potential stars later on
                                "srmag": "$candidate.srmag1",
                                "simag": "$candidate.simag1",
                                "szmag": "$candidate.szmag1",
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
            event_id = None
            for event in events:
                if event['name'] == event_name:
                    event_id = event['id']
                    break
            if event_id is None:
                print(f'ERROR: No event found with {event_name}, skipping...')
                continue
            # to each matches, add a delta_t field
            # and the distance to the event position in arcsec
            formatted_matches = []

            for match in matches:
                if is_red_star(match):
                    print(f'Found a red star candidate {match["object_id"]}, skipping...')
                    continue
                # remove the extra fields we just needed for filtering
                del match['srmag']
                del match['simag']
                del match['szmag']

                event = [e for e in events if e['name'] == event_name][0]
                obs_start = event["obs_start"] # datetime string
                jd = Time(obs_start).jd # jd

                match['delta_t'] = match['jd'] - jd
                match['distance_arcmin'] = great_circle_distance(
                    event['ra'], event['dec'], match['ra'], match['dec']
                ) * 60
                # and a distance_arcmin / pos_err ratio
                match['distance_ratio'] = match['distance_arcmin'] / (event['pos_err'] * 60)

                # compute the age
                match['age'] = match['jd'] - match['jdstarthist']
                del match['jdstarthist']
                if archival:
                    match['archival'] = 1 # mark this as archival match

                match['event_id'] = event_id # set the event_id for this match
                formatted_matches.append(match)

            results[event_name] = formatted_matches

    return results

def service(k: Kowalski) -> float:
    with get_db_connection() as conn:
        c = conn.cursor()
            
        # QUERY SOURCES FOR NEW EVENTS
        new_events, _ = fetch_events(None, c, status='pending')

        # also query events that should be reprocessed:
        # - the status is done (already queried)
        # - the obs_start is less than 24 hours ago
        # - the last_queried is more than 10 minutes ago
        # or the status is 'reprocess'
        events_to_reprocess, _ = fetch_events(None, c, can_reprocess=True)
        if not events_to_reprocess:
            events_to_reprocess = []

        if not new_events and not events_to_reprocess:
            print('No events to process or reprocess.')
            return

        print(f'Found {len(new_events)} events to process (and {len(events_to_reprocess)} to reprocess)')

        # for the new events only and those with a reprocess status, we perform archival searches
        for event in new_events + [e for e in events_to_reprocess if e['query_status'] == 'reprocess']:
            try:
                archival_results = cone_searches([event], k, archival=True)
                xmatches = archival_results[event["name"]]
                if len(xmatches) > 0:
                    print(f'Found {len(archival_results[event["name"]])} archival matches for event {event["name"]}')
                    for xmatch in xmatches:
                        try:
                            insert_xmatches([xmatch], c)
                        except Exception as e:
                            # if the xmatch already exists, we can ignore the error
                            if 'UNIQUE constraint failed' in str(e):
                                print(f'Archival xmatch {xmatch["candid"]} already exists, skipping...')
                            else:
                                print(f'Failed to insert archival xmatch {xmatch["candid"]} for event {event["name"]}: {e}')
                                traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
                print(f'Failed to process archival event {event["name"]}: {e}')
                update_event_status(event['id'], f'failed: {str(e)}', c)

            conn.commit()

        # for all events (new and those to reprocess), perform the non archival cone searches
        for event in new_events + events_to_reprocess:
            try:
                if event['query_status'] == 'pending':
                    update_event_status(event['id'], 'processing', c)

                results = cone_searches([event], k)
                
                xmatches = results[event["name"]]
                if len(xmatches) > 0:
                    print(f'Found {len(xmatches)} matches for event {event["name"]}')
                    for xmatch in xmatches:
                        try:
                            insert_xmatches([xmatch], c)
                        except Exception as e:
                            # if the xmatch already exists, we can ignore the error
                            if 'UNIQUE constraint failed' in str(e):
                                print(f'Xmatch {xmatch["candid"]} already exists, skipping...')
                            else:
                                print(f'Failed to insert xmatch {xmatch["candid"]} for event {event["name"]}: {e}')
                                traceback.print_exc()

                update_event_status(event['id'], 'done', c)
            except Exception as e:
                traceback.print_exc()
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

    print('Starting service...')
    while True:
        try:
            service(k)
        except Exception as e:
            traceback.print_exc()
            print(f'Failed to run service: {e}')
        time.sleep(5)
        print('Service loop')

