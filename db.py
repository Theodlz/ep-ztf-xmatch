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

def update_event_status(event_id: int, status: str, c: sqlite3.Cursor) -> None:
    # when we update the query_status, we also want to update the updated_at timestamp, and the last_queried timestamp
    c.execute(f"UPDATE events SET query_status=?, updated_at=CURRENT_TIMESTAMP, last_queried=CURRENT_TIMESTAMP WHERE id=?", (status, event_id))

def remove_xmatches_by_event_id(event_id: int, c: sqlite3.Cursor, keep_archival = False) -> None:
    if keep_archival:
        c.execute(f"DELETE FROM xmatches WHERE event_id=? AND archival=0", (event_id,))
    else:
        c.execute(f"DELETE FROM xmatches WHERE event_id=?", (event_id,))

def fetch_events(event_names: list, c: sqlite3.Cursor, **kwargs) -> Tuple[list, int]:
    query = 'SELECT * FROM events'
    count_query = 'SELECT COUNT(*) FROM events'
    conditions = []
    parameters = []
    if event_names is not None:
        conditions.append(' name IN ({})'.format(','.join('?'*len(event_names))))
        parameters += event_names
    elif isinstance(kwargs.get('event_ids'), list):
        # if event_ids is provided, use it to filter the events
        event_ids = kwargs.get('event_ids')
        if isinstance(event_ids, int | str | list):
            try:
                if isinstance(event_ids, int | str):
                    event_ids = [int(event_ids)]
                elif isinstance(event_ids, list):
                    event_ids = [int(event_id) for event_id in event_ids]
            except Exception as e:
                raise ValueError(f"Invalid event_ids provided: {event_ids}. Error: {e}")
        conditions.append(' id IN ({})'.format(','.join('?'*len(event_ids))))
        parameters += event_ids
    if kwargs.get('status') is not None:
        conditions.append(' query_status = ?')
        parameters.append(kwargs.get('status'))
    elif kwargs.get('can_reprocess') is not None:
        # can reprocess means that:
        # - the status is done
        # - the obs_start is less than 31 days old (to avoid getting new candidates for events that are too old)
        # - the last_queried is more than 10 minutes ago
        # OR the status is reprocess
        # conditions.append(' query_status = ?')
        # conditions.append(' obs_start > ?')
        # conditions.append(' last_queried < ?')
        conditions.append(' (query_status = ? OR (query_status = ? AND obs_start >= ? AND last_queried < ?)) ')
        parameters.append('reprocess')  # for the reprocess case
        parameters.append('done')
        parameters.append(datetime.utcnow() - timedelta(days=31))
        parameters.append(datetime.utcnow() - timedelta(minutes=10))
    if kwargs.get('latestOnly') == True:
        # latest only means that for all events with the same name, we only return the latest one (highest version)
        conditions.append(' version = (SELECT MAX(version) FROM events AS e WHERE e.name = events.name)')
        # this one above doesn't work, it seems like it just keeps the events with the highest version in the table
    if kwargs.get('matchesOnly') == True:
        #  here we only return events if they have matches in the xmatches table
        tmp_condition = "SELECT event_id FROM xmatches where event_id = events.id"
        if isinstance(kwargs.get('matchesMaxDeltaT'), int | float) and kwargs.get('matchesMaxDeltaT') > 0:
            tmp_condition +=' AND abs(xmatches.delta_t) <= ? '
            parameters.append(kwargs.get('matchesMaxDeltaT'))

        if kwargs.get('matchesOnlyIgnoreArchival', False) == True:
            tmp_condition += ' AND archival=0 '  # only consider non-archival xmatches using the ar

        tmp_condition += f' GROUP BY event_id'
        conditions.append(f' id IN ({tmp_condition}) ')
    
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

def fetch_event(event_name: str, c: sqlite3.Cursor, **kwargs) -> list:
    query = 'SELECT * FROM events'
    conditions = [' name = ?']
    parameters = [event_name]
    if kwargs.get('version') is not None:
        conditions.append(' version = ?')
        parameters.append(kwargs.get('version'))

    query += ' WHERE' + ' AND'.join(conditions)

    c.execute(query, tuple(parameters))
    return c.fetchone()

def fetch_event_by_id(event_id: int, c: sqlite3.Cursor) -> list:
    query = 'SELECT * FROM events WHERE id = ?'
    c.execute(query, (event_id,))
    return c.fetchone()

def fetch_xmatches(event_ids: list, c: sqlite3.Cursor, **kwargs) -> list:
    query = 'SELECT * FROM xmatches'
    count_query = 'SELECT COUNT(*) FROM xmatches'
    conditions = []
    parameters = []

    if isinstance(event_ids, int | str | list):
        # ensure event_ids is a list of strings
        try:
            if isinstance(event_ids, int | str):
                event_ids = [int(event_ids)]
            elif isinstance(event_ids, list):
                event_ids = [int(event_id) for event_id in event_ids]
        except Exception as e:
            raise ValueError(f"Invalid event_ids provided: {event_ids}. Error: {e}")
    else:
        event_ids = []

    if len(event_ids) > 0:
        conditions.append('event_id IN ({})'.format(','.join('?'*len(event_ids))))
        parameters += event_ids
    
    if isinstance(kwargs.get('maxDeltaT'), int | float) and kwargs.get('maxDeltaT') > 0:
        conditions.append('delta_t <= ?')
        parameters.append(kwargs.get('maxDeltaT'))
    if isinstance(kwargs.get('minDeltaT'), int | float):
        # ensure that minDeltaT is less than maxDeltaT if both are provided
        if kwargs.get('maxDeltaT', float('inf')) < kwargs.get('minDeltaT'):
            raise ValueError("maxDeltaT must be greater than or equal to minDeltaT")
        conditions.append('delta_t >= ?')
        parameters.append(kwargs.get('minDeltaT'))

    if isinstance(kwargs.get('eventAgeDays'), int | float) and kwargs.get('eventAgeDays') > 0:
        # compute the current timestamp minus the number of days specified in eventAgeDays
        # we don't return candidates if the associated event is older than the specified number of days
        conditions.append('event_id IN (SELECT id FROM events WHERE obs_start >= ?)')
        parameters.append(
            (datetime.utcnow() - timedelta(days=kwargs.get('eventAgeDays')))
        )

    if isinstance(kwargs.get('archival'), bool):
        archival = kwargs.get('archival')
        conditions.append(f'archival={1 if archival else 0}')

    if kwargs.get('deduplicateByEventName') == True:
        # since we can have the same event (same name) with different versions, we want to deduplicate the xmatches by event name
        # since means that we join the xmatches table with the events table and only keep the xmatches with the latest version of the event
        query += ' INNER JOIN events ON xmatches.event_id = events.id '
        count_query += ' INNER JOIN events ON xmatches.event_id = events.id '
        conditions.append('events.version = (SELECT MAX(version) FROM events AS e WHERE e.name = events.name)')

    if kwargs.get('to_skyportal') is not None:
        # if toSkyportal is True, we only want to return xmatches that are to be sent to SkyPortal
        # if toSkyportal is False, we only want to return xmatches that are not to be sent to SkyPortal
        conditions.append('to_skyportal = ?')
        parameters.append(1 if kwargs.get('to_skyportal') else 0)

    if kwargs.get('created_after') is not None:
        # if created_after is provided, we only want to return xmatches that are created after the specified date
        # the created_at column is a timestamp without timezone
        conditions.append('created_at >= ?')
        parameters.append(kwargs.get('created_after'))

    if kwargs.get('created_before') is not None:
        # if created_before is provided, we only want to return xmatches that are created before the specified date
        conditions.append('created_at <= ?')
        parameters.append(kwargs.get('created_before'))

    if kwargs.get('detected_after') is not None:
        # if detected_after is provided, we only want to return xmatches that are detected after the specified date
        # basically filtering on the jd column
        conditions.append('jd >= ?')
        parameters.append(kwargs.get('detected_after'))

    if len(conditions) > 0:
        query += ' WHERE ' + ' AND '.join(conditions)
        count_query += ' WHERE ' + ' AND '.join(conditions)

    count = c.execute(count_query, tuple(parameters)).fetchone()['COUNT(*)']

    if kwargs.get('order_by') is not None:
        query += f' ORDER BY {kwargs.get("order_by")}'
    else:
        query += ' ORDER BY jd DESC, object_id DESC'

    if kwargs.get('pageNumber') is not None and kwargs.get('numPerPage') is not None:
        query += f' LIMIT {kwargs.get("numPerPage")} OFFSET {(kwargs.get("pageNumber") - 1) * kwargs.get("numPerPage")}'

    xmatches = c.execute(query, tuple(parameters)).fetchall()
    return xmatches, count

def set_xmatch_as_processed(xmatch_id: int, c: sqlite3.Cursor) -> None:
    # set the xmatch as processed
    c.execute(f"UPDATE xmatches SET to_skyportal=1 WHERE id=?", (xmatch_id,))


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