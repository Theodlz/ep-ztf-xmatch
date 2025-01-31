import json
import os
import re
import time
import traceback
from functools import wraps

from astropy.time import Time
from flask import Flask, request, render_template, redirect

from db import is_db_initialized, get_db_connection, ALLOWED_EVENT_COLUMNS, insert_events, fetch_event, fetch_event_by_id, fetch_events, fetch_xmatches

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

    @app.route('/api/ping', methods=['GET'])
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
    @app.route('/api/events/<event_name>', methods=['GET'])
    @app.route('/api/events', methods=['GET', 'POST'])
    @auth()
    def event(event_name=None):
        if request.method == 'POST':
            return event_post(event_name)
        else:
            return event_get(event_name)
        
    @app.route('/api/xmatches', methods=['GET'])
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
    
    # we want an auth frontend decorator, so that if you are not authenticated, you are redirected to the login page
    def auth_frontend():
        def _auth_frontend(f):
            @wraps(f)
            def __auth_frontend(*args, **kwargs):
                # check if we have a cookie with the Authorization
                username, password = None, None
                if 'Authorization' in request.cookies:
                    username, password = request.cookies['Authorization'].split(':')
                if not username or not password:
                    if request.authorization:
                        username, password = request.authorization.username, request.authorization.password

                if not username or not password:
                    return render_template('login.html', error='Unauthorized, please login')

                if (
                    not username_regex.match(username)
                    or not username_regex.match(password)
                ):
                    render_template('login.html', error='Invalid username or password')
                with get_db_connection() as conn:
                    c = conn.cursor()
                    c.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password))
                    if c.fetchone() is None:
                        return render_template('login.html', error='Invalid username or password')
                result = f(*args, **kwargs)
                return result
            return __auth_frontend
        return _auth_frontend
    
    @app.route('/', methods=['GET'])
    @auth_frontend()
    def events_page():
        # check if we got in query parameters:
        # - pageNumber (starting at 1, default 1)
        # - numPerPage (default 100, min 1, max 1000)
        pageNumber = request.args.get('pageNumber', 1)
        numPerPage = request.args.get('numPerPage', 10)
        matchesOnly = request.args.get('matchesOnly', False)
        try:
            pageNumber = int(pageNumber)
            numPerPage = int(numPerPage)
            matchesOnly = bool(matchesOnly)
        except Exception as e:
            return {
                'message': 'Invalid query parameters',
            }, 400
        with get_db_connection() as conn:
            c = conn.cursor()
            events = fetch_events(None, c, pageNumber=pageNumber, numPerPage=numPerPage, order_by='obs_start DESC', matchesOnly=matchesOnly)
            if events is None:
                events = []
            for event in events:
                results = fetch_xmatches([event['id']], c)
                event['xmatches'] = results

            # get the total number of events
            c.execute('SELECT COUNT(*) FROM events')
            total_events = c.fetchone().get('COUNT(*)')
        return render_template(
            'events.html',
            events=events,
            pageNumber=pageNumber,
            numPerPage=numPerPage,
            totalMatches=total_events,
            totalPages=(total_events + numPerPage - 1) // numPerPage,
            matchesOnly=matchesOnly,
        )
            
    @app.route('/events/<event_name>', methods=['GET'])
    @auth_frontend()
    def event_page(event_name=None):
        if event_name is None:
            return {
                'message': 'Event name is required',
            }, 400
        with get_db_connection() as conn:
            c = conn.cursor()
            event = fetch_event(event_name, c)
            
            if event is None:
                return {
                    'message': 'Event not found',
                }, 404
            
            obs_start = event['obs_start']
            obs_start_jd = Time(obs_start).jd
            
            xmatches = fetch_xmatches([event['id']], c)
            for xmatch in xmatches:
                xmatch['delta_t'] = xmatch['jd'] - obs_start_jd
            event['xmatches'] = xmatches
            return render_template(
                'event.html',
                event=event,
                xmatches=xmatches,
            )
        
    @app.route('/login', methods=['POST'])
    # this route should get the username and password from the request, check if they are in the database, and if so set a cookie and redirect to the events page
    def login():
        # should find the login in the request params
        username = request.form.get('username')
        password = request.form.get('password')
        if (
            not username_regex.match(username)
            or not username_regex.match(password)
        ):
            return render_template('login.html')
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password))
            if c.fetchone() is None:
                return render_template('login.html')
        
        # set a cookie for the Authorization
        response = app.make_response(redirect('/'))
        response.set_cookie('Authorization', f'{username}:{password}')
        return response
    
    @app.route('/logout', methods=['POST'])
    # this route should clear the cookie and redirect to the login page
    def logout():
        response = app.make_response(redirect('/'))
        response.set_cookie('Authorization', '', expires=0)
        return response

    return app

if __name__ == "__main__":

    while not is_db_initialized():
        print('Waiting for database to be initialized...')
        time.sleep(15)

    app = make_app()

    app.debug = True
    app.run(port=4000)

