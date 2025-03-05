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

from db import is_db_initialized, get_db_connection, fetch_event, fetch_events, fetch_xmatches, fetch_archival_xmatches

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
            
    @app.route('/api/reprocess', methods=['GET', 'POST'])
    @auth()
    def reprocess():
        # admin only, we drop all the entries in the xmatches and archival_xmatches tables and reprocess all the events
        if request.user.get('type') != 'admin':
            return {
                'message': 'Unauthorized, must be an admin user',
            }, 401
        if request.method == 'POST':
            # 1. remove all the entries in the xmatches and archival_xmatches tables
            # 2. set the status of all the events to 'pending'
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute('DELETE FROM xmatches')
                c.execute('DELETE FROM archival_xmatches')
                c.execute('UPDATE events SET query_status = "pending"')
                conn.commit()
            return {
                'message': 'Reprocessing started',
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
        is_admin= request.user.get('type') == 'admin'

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
        
        if not is_admin: # if the user is NOT an admin, always ignore archival
            matchesOnlyIgnoreArchival = True
        
        now = Time.now().jd
        with get_db_connection() as conn:
            c = conn.cursor()
            events, totalMatches = fetch_events(
                None, c, pageNumber=pageNumber, numPerPage=numPerPage, order_by='obs_start DESC',
                matchesOnly=matchesOnly,
                matchesOnlyIgnoreArchival=matchesOnlyIgnoreArchival,
                latestOnly=latestOnly,
                is_admin=is_admin,
            )
            if events is None:
                events = []
            for event in events:
                count = c.execute('SELECT COUNT(*) FROM xmatches WHERE event_id = ?', (event['id'],)).fetchone()['COUNT(*)']
                event['num_xmatches'] = count
                if is_admin:
                    count = c.execute('SELECT COUNT(*) FROM archival_xmatches WHERE event_id = ?', (event['id'],)).fetchone()['COUNT(*)']
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
                is_admin=is_admin,
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
        
        is_admin = request.user.get('type') == 'admin'

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
            
            versions = c.execute('SELECT version FROM events WHERE name = ? ORDER BY version DESC', (event_name,)).fetchall()
            versions = [v['version'] for v in versions]
            
            xmatches = fetch_xmatches([event['id']], c)
            for xmatch in xmatches:
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

            # same with archival xmatches
            archival_xmatches = []
            if is_admin:
                archival_xmatches = fetch_archival_xmatches([event['id']], c)
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

            return render_template(
                'event.html',
                event=event,
                versions=versions,
                xmatches=xmatches,
                archival_xmatches=archival_xmatches,
                username=request.user.get('username'),
                is_admin=is_admin,
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

