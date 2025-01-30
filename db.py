import sqlite3
from datetime import datetime, timedelta

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def is_db_initialized():
    try:
        conn = sqlite3.connect('./data/database.db')
        c = conn.cursor()
        c.execute('SELECT * FROM users')
        conn.close()
        return True
    except sqlite3.OperationalError:
        return False

def db_init(username, password):
    # create the database
    conn = sqlite3.connect('./data/database.db')
    c = conn.cursor()

    try:
        # create the users table
        c.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                password TEXT,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (username, email)
            )
        ''')
    except sqlite3.OperationalError:
        print("Users table already exists.")

    try:
        # create the events table
        c.execute('''
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                ra REAL,
                dec REAL,
                pos_err REAL,
                obs_start TIMESTAMP,
                exp_time REAL,
                flux REAL,
                src_id INTEGER,
                src_significance REAL,
                bkg_counts REAL,
                net_counts REAL,
                net_rate REAL,
                version TEXT,
                last_queried TIMESTAMP,
                query_status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (name, version)
            )
        ''')
    except sqlite3.OperationalError:
        print("Events table already exists.")

    try:
        # create the xmatches table
        c.execute('''
            CREATE TABLE xmatches (
                id INTEGER PRIMARY KEY,
                event_id INTEGER,
                candid INTEGER,
                object_id TEXT,
                jd REAL,
                ra REAL,
                dec REAL,
                fid INTEGER,
                magpsf REAL,
                sigmapsf REAL,
                drb REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                flags TEXT,
                UNIQUE (event_id, candid)
            )
        ''')
    except sqlite3.OperationalError:
        print("xmatches table already exists.")

    # if there is no admin user yet, create one
    existing_user = c.execute('SELECT * FROM users WHERE username = "admin"').fetchone()
    if existing_user is None:
        c.execute('''
            INSERT INTO users (username, password, email)
            VALUES (?, ?, ?)
        ''', (username, password, ''))
    else:
        print("Admin user already exists.")

    # commit the changes and close the connection
    conn.commit()
    conn.close()
    return

# we want to have a context manager to create and close the database connections
from contextlib import contextmanager
@contextmanager
def get_db_connection():
    conn = sqlite3.connect('./data/database.db')
    conn.row_factory = dict_factory
    try:
        yield conn
    finally:
        conn.close()

ALLOWED_EVENT_COLUMNS = [
    'name',
    'ra',
    'dec',
    'pos_err',
    'obs_start',
    'exp_time',
    'flux',
    'src_id',
    'src_significance',
    'bkg_counts',
    'net_counts',
    'net_rate',
    'version'
]

ALLOWED_RESULT_COLUMNS = [
    'candid',
    'object_id',
    'jd',
    'ra',
    'dec',
    'fid',
    'magpsf',
    'sigmapsf',
    'drb'
]

def insert_events(events: list, c: sqlite3.Cursor, duplicate="skip") -> None:
    for event in events:
        # the obs_start is a string in the format 'YYYY-MM-DDTHH:MM:SSZ'
        # we need to convert it to a timestamp
        event['obs_start'] = datetime.strptime(event['obs_start'], '%Y-%m-%dT%H:%M:%SZ')
        query = f"INSERT INTO events ({','.join(event.keys())}) VALUES ({','.join(['?']*len(event))})"
        try:
            c.execute(query, tuple(event.values()))
        except sqlite3.IntegrityError as e:
            if duplicate == "skip":
                continue
            elif duplicate == "update":
                # update the event with the new values
                c.execute(f"UPDATE events SET {','.join([f'{k}=?' for k in event.keys()])} WHERE id=?", (*event.values(), event['id']))
            else:
                raise e

def insert_xmatches(xmatches: list, c: sqlite3.Cursor) -> None:
    for xmatch in xmatches:
        query = f"INSERT INTO xmatches ({','.join(xmatch.keys())}) VALUES ({','.join(['?']*len(xmatch))})"
        try:
            c.execute(query, tuple(xmatch.values()))
        except sqlite3.IntegrityError:
            # update the xmatch with the new values
            c.execute(f"UPDATE xmatches SET {','.join([f'{k}=?' for k in xmatch.keys()])} WHERE id=?", (*xmatch.values(), xmatch['id']))

def update_event_status(event_id: int, status: str, c: sqlite3.Cursor) -> None:
    # when we update the query_status, we also want to update the updated_at timestamp, and the last_queried timestamp
    c.execute(f"UPDATE events SET query_status=?, updated_at=CURRENT_TIMESTAMP, last_queried=CURRENT_TIMESTAMP WHERE id=?", (status, event_id))

def remove_xmatches_by_event_id(event_id: int, c: sqlite3.Cursor) -> None:
    c.execute(f"DELETE FROM xmatches WHERE event_id=?", (event_id,))

def fetch_events(event_names: list, c: sqlite3.Cursor, **kwargs) -> list:
    query = 'SELECT * FROM events'
    conditions = []
    parameters = []
    if event_names is not None:
        conditions.append(' name IN ({})'.format(','.join('?'*len(event_names))))
        parameters += event_names
    if kwargs.get('status') is not None:
        conditions.append(' query_status = ?')
        parameters.append(kwargs.get('status'))
    elif kwargs.get('can_reprocess') is not None:
        # can reprocess means that:
        # - the status is done
        # - the obs_start is less than 24 hours ago
        # - the last_queried is more than 10 minutes ago
        conditions.append(' query_status = ?')
        conditions.append(' obs_start > ?')
        conditions.append(' last_queried < ?')
        parameters.append('done')
        parameters.append(datetime.utcnow() - timedelta(hours=24))
        parameters.append(datetime.utcnow() - timedelta(minutes=10))
    
    if len(parameters) == 0:
        c.execute('SELECT * FROM events')
    else:
        query += ' WHERE' + ' AND'.join(conditions)
        c.execute(query, tuple(parameters))

    return c.fetchall()
    


def fetch_event(event_name: str, c: sqlite3.Cursor) -> list:
    query = 'SELECT * FROM events WHERE name = ?'
    c.execute(query, (event_name,))
    return c.fetchone()

def fetch_xmatches(event_ids: list, c: sqlite3.Cursor) -> list:
    event_ids = [str(event_id) for event_id in event_ids]
    if event_ids is None:
        c.execute('SELECT * FROM xmatches order by id')
    else:
        c.execute(f'SELECT * FROM xmatches WHERE event_id IN ({",".join(event_ids)}) order by id')

    return c.fetchall()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Initialize the database.')
    parser.add_argument('--init', action='store_true', help='Initialize the database.')
    parser.add_argument('--adminusername', type=str, default='admin', help='Admin username.')
    parser.add_argument('--adminpassword', type=str, default='admin', help='Admin password.')
    args = parser.parse_args()
    if args.init:
        db_init(args.adminusername, args.adminpassword)
        print("---Database initialized---")
    else:
        print("Nothing to do.")