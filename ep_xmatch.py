import os
import time
from astropy.time import Time
import traceback
import numpy as np

from penquins import Kowalski
from db import is_db_initialized, get_db_connection, fetch_events, update_event_status, insert_xmatches, remove_xmatches_by_event_id, insert_archival_xmatches

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
                                "ndethist": "$candidate.ndethist"
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

            results[event_name] = matches

    return results

def archival_cone_searches(events, k: Kowalski):
    # here instead of using Kowalski's conesearches feature, we'll use the 
    # aggregation pipeline so we can:
    # first query by position + distance
    # then filter by time, and other quality cuts (e.g., rb, drb)
    queries = []

    for event in events:
        obs_start = event["obs_start"] # datetime string
        # convert to jd
        jd = Time(obs_start).jd # jd

        # we get the midpoint of the 30 minute window
        
        jd_start = jd - DELTA_T - DELTA_T_ARCHIVAL
        jd_end = jd - DELTA_T # up to when we query the non-archival data

        # convert the radius from degrees to radians by dividing by 180/pi
        ra, dec = event["ra"], event["dec"]
        radius_rad = event["pos_err"] / (180.0 / np.pi)
        pipeline = [
            {
                "$match": {
                    "coordinates.radec_geojson": {
                        "$geoWithin": {
                            "$centerSphere": [[ra - 180.0, dec], radius_rad]
                        }
                    },
                    "candidate.jd": {
                        "$gte": jd_start,
                        "$lte": jd_end,
                    },
                    "candidate.rb": {
                        "$gt": 0.3 # remove bogus detections (random forest)
                    },
                    "candidate.drb": {
                        "$gt": 0.5 # remove bogus detections (deep learning)
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
                        }
                    ]
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "object_id": "$objectId",
                    "jd": "$candidate.jd",
                    "jdstarthist": "$candidate.jdstarthist",
                    "ndethist": "$candidate.ndethist",
                    "ra": "$candidate.ra",
                    "dec": "$candidate.dec",
                    "fid": "$candidate.fid",
                    "magpsf": "$candidate.magpsf",
                    "sigmapsf": "$candidate.sigmapsf",
                    "drb": "$candidate.drb",
                    "sgscore": "$candidate.sgscore1",
                }
            },
            # then sort by jd desc
            {
                "$sort": {
                    "jd": -1
                }
            },
            # then group by object_id, keeping only the first one (the most recent),
            {
                "$group": {
                    "_id": "$object_id",
                    "last_detected_jd": {"$first": "$jd"},
                    "last_detected_magpsf": {"$first": "$magpsf"},
                    "last_detected_sigmapsf": {"$first": "$sigmapsf"},
                    "last_detected_fid": {"$first": "$fid"},
                    "last_detected_drb": {"$first": "$drb"},
                    "jdstarthist": {"$first": "$jdstarthist"},
                    "ndethist": {"$first": "$ndethist"},
                    "sgscore": {"$first": "$sgscore"},
                    "ra": {"$first": "$ra"},
                    "dec": {"$first": "$dec"},
                }
            },
            # add the event name to each object, then use a group stage again
            # so the final output is one document, with the event name and the
            # list of objects
            {
                "$addFields": {
                    "event_name": event["name"]
                }
            },
            {
                "$group": {
                    "_id": "$event_name",
                    "objects": {"$push": {
                        "object_id": "$_id",
                        "last_detected_jd": "$last_detected_jd",
                        "last_detected_magpsf": "$last_detected_magpsf",
                        "last_detected_sigmapsf": "$last_detected_sigmapsf",
                        "last_detected_fid": "$last_detected_fid",
                        "last_detected_drb": "$last_detected_drb",
                        "jdstarthist": "$jdstarthist",
                        "ndethist": "$ndethist",
                        "sgscore": "$sgscore",
                        "ra": "$ra",
                        "dec": "$dec",
                    }}
                }
            }
        ]

        queries.append(
            {
                "query_type": "aggregate",
                "query": {
                    "catalog": "ZTF_alerts",
                    "pipeline": pipeline,
                }
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

        data = response.get('data', [])

        if len(data) == 0:
            continue

        event_name, matches = data[0].get('_id'), data[0].get('objects', [])
        event = [e for e in events if e['name'] == event_name][0]
        obs_start = event["obs_start"] # datetime string
        jd = Time(obs_start).jd # jd
        # to each matches, add a delta_t field
        # and the distance to the event position in arcsec
        for match in matches:
            match['delta_t'] = match['last_detected_jd'] - jd
            match['distance_arcmin'] = great_circle_distance(
                event['ra'], event['dec'], match['ra'], match['dec']
            ) * 60
            # and a distance_arcmin / pos_err ratio
            match['distance_ratio'] = match['distance_arcmin'] / (event['pos_err'] * 60)

            # compute the age for each
            match['last_detected_age'] = match['last_detected_jd'] - match['jdstarthist']
            del match['jdstarthist']
        results[event_name] = matches

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
        events_to_reprocess, _ = fetch_events(None, c, can_reprocess=True)
        if not events_to_reprocess:
            events_to_reprocess = []

        events = new_events + events_to_reprocess
        if not events:
            print('No events to process')
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
                traceback.print_exc()
                print(f'Failed to process event {event["name"]}: {e}')
                update_event_status(event['id'], f'failed: {str(e)}', c)
        conn.commit()

        # for the new events only, we perform archival searches
        for event in new_events:
            try:
                results = archival_cone_searches([event], k)
                print(f'Found {len(results[event["name"]])} archival matches for event {event["name"]}')

                xmatches = results[event["name"]]
                if len(xmatches) > 0:
                    for xmatch in xmatches:
                        xmatch['event_id'] = event['id']
                    insert_archival_xmatches(xmatches, c)
            except Exception as e:
                traceback.print_exc()
                print(f'Failed to process archival event {event["name"]}: {e}')
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

