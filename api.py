import json
import os
import re
import time
import traceback
from functools import wraps

from gevent import monkey
monkey.patch_all()

from astropy.time import Time
from flask import Flask, request, render_template, redirect

from db import is_db_initialized, get_db_connection, ALLOWED_EVENT_COLUMNS, insert_events, fetch_event, fetch_events, fetch_xmatches

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'

# only allow alphanumeric characters and underscores in the username and password
username_regex = re.compile(r'^[a-zA-Z0-9_]+$')
email_regex = re.compile(r'^[a-zA-Z0-9_]+@[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$')

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
                existing_user = c.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password)).fetchone()
                if existing_user is None:
                    return 'Unauthorized', 401
                
                request.user = existing_user

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
                events, count = fetch_events(None, c)
                return {
                    'message': 'Events found',
                    'data': {
                        'events': events,
                        'totalMatches': count,
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
    
    @app.route('/api/users', methods=['GET', 'POST'])
    @auth()
    def users():
        if request.user.get('type') != 'admin':
            return {
                'message': 'Unauthorized, must be an admin user',
            }, 401
        if request.method == 'POST':
            data = request.data
            user = None
            try:
                user = json.loads(data)
            except Exception as e:
                return {
                    'message': 'Failed to read data',
                }, 400
            
            missing_columns = [col for col in ['username', 'password', 'email', 'type'] if col not in user]
            if missing_columns:
                return {
                    'message': f'Missing required columns: {missing_columns}',
                }, 400
            
            if (
                not username_regex.match(user['username'])
                or not username_regex.match(user['password'])
            ):
                return {
                    'message': 'Invalid username or password',
                }, 400
            
            if email_regex.match(user['email']) is None:
                return {
                    'message': 'Invalid email',
                }, 400
            
            if user.get('type') not in ['normal', 'admin']:
                return {
                    'message': 'Invalid user type',
                }, 400
            
            
            with get_db_connection() as conn:
                c = conn.cursor()
                existing_user = c.execute('SELECT * FROM users WHERE username = ?', (user['username'],)).fetchone()
                if existing_user is not None:
                    return {
                        'message': 'User already exists',
                    }, 400
                
                try:
                    c.execute('''
                        INSERT INTO users (username, password, email, type)
                        VALUES (?, ?, ?, ?)
                    ''', (user['username'], user['password'], user['email'], user['type']))
                    conn.commit()
                except Exception as e:
                    return {
                        'message': f'Failed to insert user: {e}',
                    }, 400
                
            return {
                'message': 'User inserted successfully',
            }
        else:
            with get_db_connection() as conn:
                c = conn.cursor()
                users = c.execute('SELECT * FROM users').fetchall()

            # remove the passwords
            for user in users:
                del user['password']
            return {
                'message': 'Users found',
                'data': {
                    'users': users,
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
                    return render_template('login.html', error='Unauthorized, please login')

                if (
                    not username_regex.match(username)
                    or not username_regex.match(password)
                ):
                    render_template('login.html', error='Invalid username or password')
                with get_db_connection() as conn:
                    c = conn.cursor()
                    existing_user = c.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password)).fetchone()
                    if existing_user is None:
                        return render_template('login.html', error='Invalid username or password')
                    
                    request.user = existing_user
                    
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
            matchesOnly = bool(str(matchesOnly).lower() == 'true')
        except Exception as e:
            return {
                'message': 'Invalid query parameters',
            }, 400
        
        now = Time.now().jd
        with get_db_connection() as conn:
            c = conn.cursor()
            events, totalMatches = fetch_events(None, c, pageNumber=pageNumber, numPerPage=numPerPage, order_by='obs_start DESC', matchesOnly=matchesOnly)
            if events is None:
                events = []
            for event in events:
                count = c.execute('SELECT COUNT(*) FROM xmatches WHERE event_id = ?', (event['id'],)).fetchone()['COUNT(*)']
                event['num_xmatches'] = count
                dt = (now - Time(event['obs_start']).jd) * 24
                if dt < 24:
                    event['delta_t'] = f"<{int(dt + 0.5)}h"
                else:
                    event['delta_t'] = ">24h"

        try:
            template_rendered = render_template(
                'events.html',
                events=events,
                pageNumber=pageNumber,
                numPerPage=numPerPage,
                totalMatches=totalMatches,
                totalPages=(totalMatches + numPerPage - 1) // numPerPage,
                matchesOnly=matchesOnly,
                username=request.user.get('username'),
                is_admin=request.user.get('type') == 'admin',
            )
            return template_rendered
        except Exception as e:
            return {
                'message': f'Failed to render template: {e}',
            }, 500
            
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
                username=request.user.get('username'),
                is_admin=request.user.get('type') == 'admin',
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
            return render_template('login.html', error='Invalid username or password')
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password))
            if c.fetchone() is None:
                return render_template('login.html', error='Invalid username or password')
        
        # set a cookie for the Authorization
        response = app.make_response(redirect('/'))
        response.set_cookie('Authorization', f'{username}:{password}', max_age=60*60*24) # 1 day
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

