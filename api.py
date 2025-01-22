import io
import json
import os
import uuid
from functools import wraps

import pandas as pd
from flask import Flask, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'

username = os.getenv('API_USERNAME', 'admin')
password = os.getenv('API_PASSWORD', 'admin')

def auth():
    def _auth(f):
        @wraps(f)
        def __auth(*args, **kwargs):
            if not request.authorization or request.authorization.username != username or request.authorization.password != password:
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

    @app.route('/ep_xmatch', methods=['POST'])
    @auth()
    def ep_xmatch():
        data = request.data

        events = None
        try:
            json_data = json.loads(data)
            events = pd.json_normalize(json_data)
        except Exception as e:
            pass
        if events is None:
            try:
                events = pd.read_csv(io.BytesIO(data))
            except Exception as e:
                pass
        if events is None:
            return {
                'message': 'Failed to read data',
            }, 400
        
        request_id = str(uuid.uuid4())
        events.to_csv(f'{BASE_DIR}/request_{request_id}.csv', index=False)

        if not os.path.exists(f'{BASE_DIR}/requests.txt'):
            with open(f'{BASE_DIR}/requests.txt', 'w') as f:
                pass

        with open(f'{BASE_DIR}/requests.txt', 'a') as f:
            f.write(f'{request_id},pending\n')

        return {
            'message': 'Request received',
            'data': {'id': request_id},
        }
    
    @app.route('/ep_xmatch/<request_id>', methods=['GET'])
    @auth()
    def ep_xmatch_status(request_id):
        with open(f'{BASE_DIR}/requests.txt', 'r') as f:
            lines = f.readlines()
        
        for line in lines:
            req_id, status = line.strip().split(',')
            if req_id == request_id:
                break
        else:
            return {
                'message': 'Request not found'
            }, 404
        
        if status in ['pending', 'processing']:
            return {
                'message': f'Request {status}'
            }, 200
        elif status == 'done':
            with open(f'{BASE_DIR}/request_{request_id}.json', 'r') as f:
                results = json.load(f)
            return {
                'message': 'Request done',
                'data': results,
            }
        else:
            return {
                'message': 'Request failed'
            }, 400

    return app

if __name__ == "__main__":
    app = make_app()

    app.debug = True
    app.run(port=4000)

