import json
import os
import time

import pandas as pd
from penquins import Kowalski

FID_TO_BAND = {
    '1': 'g',
    '2': 'r',
    '3': 'i',
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'
    
def fid_to_band(fid):
    if pd.isna(fid):
        return fid
    return FID_TO_BAND.get(str(int(fid)), pd.NA)

def cone_searches(events: list, k: Kowalski):
    queries = []

    results = {}

    for event in events:
        jd_start = event["jd"] - 0.5
        jd_end = event["jd"] + 0.5
        queries.append(
            {
                "query_type": "cone_search",
                "query": {
                    "object_coordinates": {
                        "radec": {
                            str(event["id"]): [
                                event["ra"],
                                event["dec"]
                            ]
                        },
                        "cone_search_radius": event["radius"],
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
                                "objectId": 1,
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
        int(event["id"]): [] for event in events
    }

    for response in responses.get('default', []):
        if response.get('status') != 'success':
            print(f'Failed to get objects at positions: {response.get("message", "")}')
            continue

        for id, matches in response.get('data', {}).get('ZTF_alerts', {}).items():
            results[int(id)] = matches

    return results


def update_status(request_id, status):
    with open(f'{BASE_DIR}/requests.txt', 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            _request_id, _status = line.strip().split(',')
            if _request_id == request_id:
                lines[i] = f'{request_id},{status}\n'

    with open(f'{BASE_DIR}/requests.txt', 'w') as f:
        f.writelines(lines)

    print(f'Updated status of {request_id} to {status}')

def service(k: Kowalski):
    selected_request_id, filepath = None, None
    with open(f'{BASE_DIR}/requests.txt', 'r') as f:
        for line in f.readlines():
            request_id, status = line.strip().split(',')
            if status == 'pending':
                selected_request_id = request_id
                filepath = f'{BASE_DIR}/request_{request_id}.csv'

    if filepath is None:
        return

    # if the file doesn't exist, mark the request as failed
    if not os.path.exists(filepath):
        print(f'File {filepath} does not exist')
        update_status(selected_request_id, 'failed')
        return

    # read the csv file
    try:
        events = pd.read_csv(filepath).to_dict(orient='records')
    except Exception as e:
        print(f'Failed to read: {e}')
        update_status(selected_request_id, 'failed')
        return

    update_status(selected_request_id, 'processing')

    # process the request
    results = cone_searches(events, k)

    # write them to a json file with the same name as the request_id
    with open(f'{BASE_DIR}/request_{selected_request_id}.json', 'w') as f:
        f.write(json.dumps(results))

    # mark the request as done
    update_status(selected_request_id, 'done')

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

    if not os.path.exists(f'{BASE_DIR}/requests.txt'):
        os.makedirs(BASE_DIR, exist_ok=True)
        with open(f'{BASE_DIR}/requests.txt', 'w') as f:
            pass

    while True:
        service(k)
        time.sleep(1)
        print('Service loop')

