import json
import os
import re
import time
from functools import wraps

from gevent import monkey
monkey.patch_all()

from astropy.time import Time
from flask import Flask, request, render_template, redirect

from db import is_db_initialized, get_db_connection, fetch_event, fetch_events, fetch_xmatches

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'

DT_XMATCH_NONADMIN = 60.0 # in minutes
DT_XMATCH_NONADMIN = float(os.getenv('DT_XMATCH_NONADMIN', DT_XMATCH_NONADMIN))
# convert from minutes to days
DT_XMATCH_NONADMIN /= 60 * 24

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
    
    @app.route('/api/users', methods=['GET', 'POST'])
    @auth()
    def users():
        if request.user.get('type') != 'caltech':
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
            
            if user.get('type') not in ['external', 'partner', 'caltech']:
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
            
    @app.route('/api/reprocess', methods=['GET', 'POST'])
    @auth()
    def reprocess():
        # admin only, we drop all the entries in the xmatches and reprocess all the events
        if request.user.get('type') != 'caltech':
            return {
                'message': 'Unauthorized, must be an admin user',
            }, 401
        if request.method == 'POST':
            # 1. remove all the entries in the xmatches
            # 2. set the status of all the events to 'reprocess'
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute('DELETE FROM xmatches')
                c.execute('UPDATE events SET query_status = "reprocess"')
                conn.commit()
            return {
                'message': 'Reprocessing started',
            }
        
    # add an endpoint where given an event's name and version (defaults to latest), we return the event's details
    # and associated xmatches
    # this endpoint should be authenticated
    @app.route('/api/events/<event_name>', methods=['GET'])
    @auth()
    def api_event(event_name=None):
        is_caltech = request.user.get('type') == 'caltech'
        if request.user.get('type') != 'caltech':
            return {
                'message': 'Unauthorized, must be an admin user',
            }, 401
        if event_name is None:
            return {
                'message': 'Event name is required',
            }, 400
        version = request.args.get('version', None)
        if version == '':
            version = None
        if version is not None:
            # version should be v + number
            if not version.startswith('v'):
                return {
                    'message': 'Invalid version',
                }, 400
            elif not version[1:].isdigit():
                return {
                    'message': 'Invalid version',
                }, 400
        with get_db_connection() as conn:
            c = conn.cursor()
            event = fetch_event(
                event_name, c,
                version=version,
            )
            if event is None:
                return {
                    'message': 'Event not found',
                }, 404
            xmatches, _ = fetch_xmatches(
                [event['id']], c,
                maxDeltaT=DT_XMATCH_NONADMIN if not is_caltech else None,
                minDeltaT=-DT_XMATCH_NONADMIN if not is_caltech else None,
                archival=False
            )
            event['xmatches'] = xmatches
            if is_caltech:
                archival_xmatches, _ = fetch_xmatches(
                    [event['id']], c,
                    archival=True
                )
                event['archival_xmatches'] = archival_xmatches
            return {
                'message': 'Event found',
                'data': event,
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
    def index():
        return redirect('/events')

    @app.route('/events', methods=['GET'])
    @auth_frontend()
    def events_page():
        user_type = request.user.get('type')

        # check if we got in query parameters:
        # - pageNumber (starting at 1, default 1)
        # - numPerPage (default 100, min 1, max 1000)
        pageNumber = request.args.get('pageNumber', 1)
        numPerPage = request.args.get('numPerPage', 10)
        matchesOnly = request.args.get('matchesOnly', False)
        matchesOnlyIgnoreArchival = request.args.get('matchesOnlyIgnoreArchival', False)
        latestOnly = request.args.get('latestOnly', True)
        try:
            pageNumber = int(pageNumber)
            numPerPage = int(numPerPage)
            matchesOnly = bool(str(matchesOnly).lower() == 'true')
            matchesOnlyIgnoreArchival = bool(str(matchesOnlyIgnoreArchival).lower() == 'true')
            latestOnly = bool(str(latestOnly).lower() == 'true')
        except Exception as e:
            return {
                'message': 'Invalid query parameters',
            }, 400
        
        if user_type not in ["caltech"]: # always ignore events without xmatches
            matchesOnly = True
            matchesOnlyIgnoreArchival = True
        
        now = Time.now().jd
        with get_db_connection() as conn:
            c = conn.cursor()
            events, totalMatches = fetch_events(
                None, c, pageNumber=pageNumber, numPerPage=numPerPage, order_by='obs_start DESC',
                matchesOnly=matchesOnly,
                matchesOnlyIgnoreArchival=matchesOnlyIgnoreArchival,
                matchesMaxDeltaT=DT_XMATCH_NONADMIN if user_type not in ["caltech"] else None,
                latestOnly=latestOnly,
                user_type=user_type,
            )
            if events is None:
                events = []
            for event in events:
                if user_type not in ["caltech"]:
                    # for non admins we don't show archival xmatches
                    # and we limit to matches where the delta T is <= MAX_DT_XMATCH_NONADMIN
                    count = c.execute('SELECT COUNT(*) FROM xmatches WHERE event_id = ? AND abs(delta_t) <= ? AND archival = 0', (event['id'], DT_XMATCH_NONADMIN)).fetchone()['COUNT(*)']
                    event['num_xmatches'] = count
                else:
                    count = c.execute('SELECT COUNT(*) FROM xmatches WHERE event_id = ? AND archival = 0', (event['id'],)).fetchone()['COUNT(*)']
                    event['num_xmatches'] = count
                    count = c.execute('SELECT COUNT(*) FROM xmatches WHERE event_id = ? AND archival = 1', (event['id'],)).fetchone()['COUNT(*)']
                    event['num_archival_xmatches'] = count
                
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
                matchesOnlyIgnoreArchival=matchesOnlyIgnoreArchival,
                latestOnly=latestOnly,
                username=request.user.get('username'),
                user_type=user_type,
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
        
        user_type = request.user.get('type')

        version = request.args.get('version', None)
        if version == '':
            version = None
        if version is not None:
            # version should be v + number
            if not version.startswith('v'):
                return {
                    'message': 'Invalid version',
                }, 400
            elif not version[1:].isdigit():
                return {
                    'message': 'Invalid version',
                }, 400

        with get_db_connection() as conn:
            c = conn.cursor()
            event = fetch_event(
                event_name, c,
                version=version,
            )
            if event is None:
                return {
                    'message': 'Event not found',
                }, 404
            
            # to the event, we add the time in JD
            event['obs_start_jd'] = Time(event['obs_start']).jd
            
            versions = c.execute('SELECT version FROM events WHERE name = ? ORDER BY version DESC', (event_name,)).fetchall()
            versions = [v['version'] for v in versions]
            
            xmatches, _ = fetch_xmatches(
                [event['id']], c,
                maxDeltaT=DT_XMATCH_NONADMIN if user_type not in ["caltech"] else None,
                archival=False
            )
            for xmatch in xmatches:
                dt = float(xmatch['delta_t'])
                dt_abs = abs(dt)
                dt_text = None
                # if it's less than 1 minute, show in seconds
                if dt_abs < 1/24/60:
                    dt_text = f"{int(dt_abs * 24 * 60 * 60 + 0.5)}s"
                # if it's less than 1 hour, show in minutes
                if dt_abs < 1/24:
                    dt_text = f"{int(dt_abs * 24 * 60 + 0.5)}m"
                # if it's less than 1 day, show in hours
                elif dt_abs < 1:
                    dt_text = f"{int(dt_abs * 24 + 0.5)}h"
                # else show in days
                else:
                    dt_text = f"{int(dt_abs + 0.5)}d"

                if dt < 0:
                    dt_text = f"-{dt_text}"
                xmatch['delta_t'] = dt_text

                # we add the time in UTC
                xmatch['utc'] = Time(xmatch['jd'], format='jd').to_datetime().strftime('%Y-%m-%d %H:%M:%S')

            # same with archival xmatches
            archival_xmatches = []
            if user_type in ["caltech"]:
                archival_xmatches, _ = fetch_xmatches(
                    [event['id']], c,
                    archival=True
                )
                for xmatch in archival_xmatches:
                    dt = float(xmatch['delta_t'])
                    # if it's less than 1 hour, show in minutes
                    if abs(dt) < 1/24:
                        xmatch['delta_t'] = f"{int(dt * 24 * 60 + 0.5)}m"
                    # if it's less than 1 day, show in hours
                    elif abs(dt) < 1:
                        xmatch['delta_t'] = f"{int(dt * 24 + 0.5)}h"
                    # else show in days
                    else:
                        xmatch['delta_t'] = f"{int(dt + 0.5)}d"

                    # we add the time in UTC
                    xmatch['utc'] = Time(xmatch['jd'], format='jd').utc.isot

            return render_template(
                'event.html',
                event=event,
                versions=versions,
                xmatches=xmatches,
                archival_xmatches=archival_xmatches,
                username=request.user.get('username'),
                user_type=user_type,
            )
    
    @app.route('/candidates', methods=['GET'])
    @auth_frontend()
    def candidates_page():
        # the candidates page lists all xmatches, ordered by JD descending, and with their associated event information
        user_type = request.user.get('type')    
        if user_type not in ["partner", "caltech"]:
            return {
                'message': 'Unauthorized, must be a partner or caltech user',
            }, 401
        
        pageNumber = request.args.get('pageNumber', 1)
        numPerPage = request.args.get('numPerPage', 10)

        try:
            pageNumber = int(pageNumber)
            numPerPage = int(numPerPage)
        except Exception as e:
            return {
                'message': 'Invalid query parameters',
            }, 400
    
        with get_db_connection() as conn:
            c = conn.cursor()
            candidates, totalMatches = fetch_xmatches(
                None, c,
                pageNumber=pageNumber,
                numPerPage=numPerPage,
            )
            if candidates is None:
                candidates = []
            event_ids = set() # to fetch event information for the candidates
            for candidate in candidates:
                # collect the event ids for the candidates
                if 'event_id' in candidate:
                    event_ids.add(candidate['event_id'])

            events, _ = fetch_events(
                None, c, 
                latestOnly=True, # we want all events, not just the latest
                event_ids=list(event_ids), # only fetch events that are in the candidates
            )

            # convert to a hashmap for quick access
            events = {
                event['id']: event
                for event in events
            }
            # add event information to each candidate
            for candidate in candidates:
                candidate['event'] = events.get(candidate['event_id'], None)

                dt = float(candidate['delta_t'])
                dt_abs = abs(dt)
                dt_text = None
                # if it's less than 1 minute, show in seconds
                if dt_abs < 1/24/60:
                    dt_text = f"{int(dt_abs * 24 * 60 * 60 + 0.5)}s"
                # if it's less than 1 hour, show in minutes
                if dt_abs < 1/24:
                    dt_text = f"{int(dt_abs * 24 * 60 + 0.5)}m"
                # if it's less than 1 day, show in hours
                elif dt_abs < 1:
                    dt_text = f"{int(dt_abs * 24 + 0.5)}h"
                # else show in days
                else:
                    dt_text = f"{int(dt_abs + 0.5)}d"

                if dt < 0:
                    dt_text = f"-{dt_text}"
                candidate['delta_t_str'] = dt_text

                # we add the time in UTC
                candidate['utc'] = Time(candidate['jd'], format='jd').to_datetime().strftime('%Y-%m-%d %H:%M:%S')
            
            return render_template(
                'candidates.html',
                candidates=candidates,
                pageNumber=pageNumber,
                numPerPage=numPerPage,
                totalMatches=totalMatches,
                totalPages=(totalMatches + numPerPage - 1) // numPerPage,
                username=request.user.get('username'),
                user_type=user_type,
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

