import sqlite3
from datetime import datetime, timedelta
from typing import Tuple

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

    # check if the database is already initialized
    if not is_db_initialized():
        print("Database not initialized. Creating tables...")
        from migrate import run_migrations
        run_migrations()
    else:
        print("Database already initialized. Skipping table creation.")

    # if there is no admin user yet, create one
    existing_user = c.execute('SELECT * FROM users WHERE username = ? AND type = ?', (username, 'admin')).fetchone()
    if existing_user is None:
        c.execute('''
            INSERT INTO users (username, password, email, type)
            VALUES (?, ?, ?, ?)
        ''', (username, password, '', 'admin'))
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

def insert_archival_xmatches(archival_xmatches: list, c: sqlite3.Cursor) -> None:
    for archival_xmatch in archival_xmatches:
        query = f"INSERT INTO archival_xmatches ({','.join(archival_xmatch.keys())}) VALUES ({','.join(['?']*len(archival_xmatch))})"
        try:
            c.execute(query, tuple(archival_xmatch.values()))
        except sqlite3.IntegrityError:
            # update the xmatch with the new values
            c.execute(f"UPDATE archival_xmatches SET {','.join([f'{k}=?' for k in archival_xmatch.keys()])} WHERE id=?", (*archival_xmatch.values(), archival_xmatch['id']))

def update_event_status(event_id: int, status: str, c: sqlite3.Cursor) -> None:
    # when we update the query_status, we also want to update the updated_at timestamp, and the last_queried timestamp
    c.execute(f"UPDATE events SET query_status=?, updated_at=CURRENT_TIMESTAMP, last_queried=CURRENT_TIMESTAMP WHERE id=?", (status, event_id))

def remove_xmatches_by_event_id(event_id: int, c: sqlite3.Cursor) -> None:
    c.execute(f"DELETE FROM xmatches WHERE event_id=?", (event_id,))

def fetch_events(event_names: list, c: sqlite3.Cursor, **kwargs) -> Tuple[list, int]:
    query = 'SELECT * FROM events'
    count_query = 'SELECT COUNT(*) FROM events'
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
    if kwargs.get('latestOnly') == True:
        # latest only means that for all events with the same name, we only return the latest one (highest version)
        conditions.append(' version = (SELECT MAX(version) FROM events AS e WHERE e.name = events.name)')
        # this one above doesn't work, it seems like it just keeps the events with the highest version in the table
    if kwargs.get('matchesOnly') == True:
        #  here we only return events if they have matches in the xmatches table
        if kwargs.get('matchesOnlyIgnoreArchival') == True:
            # if the user querying isn't an admin:
            # don't account for archival_xmatches
            conditions.append(' id IN (SELECT event_id FROM xmatches where event_id = events.id GROUP BY event_id)')
        else:
            conditions.append('(id IN (SELECT event_id FROM xmatches where event_id = events.id GROUP BY event_id) OR id IN (SELECT event_id FROM archival_xmatches where event_id = events.id GROUP BY event_id))')
    
    if len(conditions) > 0:
        query += ' WHERE' + ' AND'.join(conditions)
        count_query += ' WHERE' + ' AND'.join(conditions)

    if kwargs.get('order_by') is not None:
        query += f' ORDER BY {kwargs.get("order_by")}'
    
    if kwargs.get('pageNumber') is not None and kwargs.get('numPerPage') is not None:
        query += f' LIMIT {kwargs.get("numPerPage")} OFFSET {(kwargs.get("pageNumber") - 1) * kwargs.get("numPerPage")}'

    count = c.execute(count_query, tuple(parameters)).fetchone()['COUNT(*)']
    events = c.execute(query, tuple(parameters)).fetchall()

    return events, count
    


def fetch_event(event_name: str, c: sqlite3.Cursor) -> list:
    query = 'SELECT * FROM events WHERE name = ?'
    c.execute(query, (event_name,))
    return c.fetchone()

def fetch_event_by_id(event_id: int, c: sqlite3.Cursor) -> list:
    query = 'SELECT * FROM events WHERE id = ?'
    c.execute(query, (event_id,))
    return c.fetchone()

def fetch_xmatches(event_ids: list, c: sqlite3.Cursor) -> list:
    event_ids = [str(event_id) for event_id in event_ids]
    if event_ids is None:
        c.execute('SELECT * FROM xmatches order by id')
    else:
        c.execute(f'SELECT * FROM xmatches WHERE event_id IN ({",".join(event_ids)}) order by jd desc, object_id desc')

    return c.fetchall()

def fetch_archival_xmatches(event_ids: list, c: sqlite3.Cursor) -> list:
    event_ids = [str(event_id) for event_id in event_ids]
    if event_ids is None:
        c.execute('SELECT * FROM archival_xmatches order by id')
    else:
        c.execute(f'SELECT * FROM archival_xmatches WHERE event_id IN ({",".join(event_ids)}) order by last_detected_jd desc, object_id desc')

    return c.fetchall()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Initialize the database.')
    parser.add_argument('--init', action='store_true', help='Initialize the database.')
    parser.add_argument('--adminusername', type=str, default='admin', help='Admin username.')
    parser.add_argument('--adminpassword', type=str, default='admin', help='Admin password.')
    args = parser.parse_args()
    if args.init:
        print("---Initializing database---")
        db_init(args.adminusername, args.adminpassword)
        print("---Completed database initialization---")
    else:
        print("Nothing to do.")