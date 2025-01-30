import json
import os
import re
import time
import traceback
from functools import wraps

from flask import Flask, request

from db import is_db_initialized, get_db_connection, ALLOWED_EVENT_COLUMNS, insert_events, fetch_event, fetch_events, fetch_xmatches

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'

# only allow alphanumeric characters and underscores in the username and password
username_regex = re.compile(r'^[a-zA-Z0-9_]+$')

def auth():
    def _auth(f):
        @wraps(f)
        def __auth(*args, **kwargs):
            if not request.authorization:
                return 'Unauthorized', 401
            username, password = request.authorization.username, request.authorization.password
            if (
                not username_regex.match(username)
                or not username_regex.match(password)
            ):
                return 'Unauthorized', 401
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password))
                if c.fetchone() is None:
                    return 'Unauthorized', 401

            result = f(*args, **kwargs)
            return result
        return __auth
    return _auth

def make_app():
    app = Flask(__name__)

    @app.route('/')
    def ping():
        return 'pong'

    def event_post(event_name=None):
        data = request.data

        events = None
        try:
            events = json.loads(data)
        except Exception as e:
            return {
                'message': 'Failed to read data',
            }, 400

        # check that they each have the allowed columns we need
        for event in events:
            if not all([col in event for col in ALLOWED_EVENT_COLUMNS]):
                return {
                    'message': 'One or more events are missing required columns',
                }, 400
        
        with get_db_connection() as conn:
            c = conn.cursor()
            try:
                insert_events(events, c)
                conn.commit()
            except Exception as e:
                traceback.print_exc()
                return {
                    'message': f'Failed to insert events: {e}',
                }, 400
            
        return {
            'message': 'Events inserted successfully',
        }
    
    def event_get(event_name=None):
        with get_db_connection() as conn:
            c = conn.cursor()
            if event_name is None:
                events = fetch_events(None, c)
                return {
                    'message': 'Events found',
                    'data': {
                        'events': events,
                    }
                }

            event = fetch_event(event_name, c)
        
            if event is None:
                return {
                    'message': 'Event not found',
                }, 404
            
            # if the status isn't done, return the status
            if event['query_status'] != 'done':
                return {
                    'message': f'Event is still being processed: {event["query_status"]}',
                }, 202
            else:
                results = fetch_xmatches([event['id']], c)
                event['xmatches'] = results
        
            return {
                'message': 'Event found',
                'data': {
                    'event': event,
                }
            }
        
    # the route above doesn't work, as the event_name should be only for the GET method
    # i.e. it needs to be optional for the POST method
    @app.route('/events/<event_name>', methods=['GET'])
    @app.route('/events', methods=['GET', 'POST'])
    @auth()
    def event(event_name=None):
        print(request.method)
        if request.method == 'POST':
            return event_post(event_name)
        else:
            return event_get(event_name)
        
    @app.route('/xmatches', methods=['GET'])
    @auth()
    def xmatches():
        with get_db_connection() as conn:
            c = conn.cursor()
            # we return all the xmatches
            xmatches = c.execute('SELECT * FROM xmatches').fetchall()
        return {
            'message': 'Xmatches found',
            'data': {
                'xmatches': xmatches,
            }
        }

    return app

if __name__ == "__main__":

    while not is_db_initialized():
        print('Waiting for database to be initialized...')
        time.sleep(15)

    app = make_app()

    app.debug = True
    app.run(port=4000)

