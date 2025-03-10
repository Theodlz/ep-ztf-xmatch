import os
import time
import traceback
import requests

from penquins import Kowalski
from db import is_db_initialized, get_db_connection, insert_events, ALLOWED_EVENT_COLUMNS

EP_BASE_URL = "https://ep.bao.ac.cn/ep"
EP_TOKEN_URL = f"{EP_BASE_URL}/api/get_tokenp"
EP_EVENTS_URL = f"{EP_BASE_URL}/data_center/api/unverified_candidates"

EP_EMAIL = os.getenv('EP_EMAIL')
EP_PASSWORD = os.getenv('EP_PASSWORD')

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
                print(f'Inserting {len(new_events)} events (skips existing ones)')
                try:
                    insert_events(new_events, c)
                    conn.commit()
                except Exception as e:
                    traceback.print_exc()
                    print(f'Failed to insert events: {e}')

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

    print('Starting service...')
    while True:
        try:
            last_event_fetch = service(k, last_event_fetch)
        except Exception as e:
            traceback.print_exc()
            print(f'Failed to run service: {e}')
        time.sleep(5)
        print('Service loop')

