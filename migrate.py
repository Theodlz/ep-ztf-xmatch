import sqlite3

def migration1():
    conn = sqlite3.connect('./data/database.db')
    c = conn.cursor()

    try:
        c.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                password TEXT,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                type TEXT DEFAULT 'normal' CHECK (type IN ('normal', 'admin')),
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
                delta_t REAL,
                distance_arcmin REAL,
                distance_ratio REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                flags TEXT,
                UNIQUE (event_id, candid)
            )
        ''')
    except sqlite3.OperationalError:
        print("xmatches table already exists.")

    conn.commit()
    conn.close()
    return

def migration2():
    conn = sqlite3.connect('./data/database.db')
    c = conn.cursor()

    # add the age column to the xmatches table
    try:
        c.execute('ALTER TABLE xmatches ADD COLUMN age REAL')
    except sqlite3.OperationalError:
        print("xmatches table already has age column.")
    # add the sgscore column to the xmatches table
    try:
        c.execute('ALTER TABLE xmatches ADD COLUMN sgscore REAL')
    except sqlite3.OperationalError:
        print("xmatches table already has sgscore column.")

    # create the archival_xmatches table
    try:
        c.execute('''
            CREATE TABLE archival_xmatches (
                id INTEGER PRIMARY KEY,
                object_id TEXT,
                event_id INTEGER,
                last_detected_jd REAL,
                last_detected_magpsf REAL,
                last_detected_sigmapsf REAL,
                last_detected_fid INTEGER,
                last_detected_drb REAL,
                last_detected_age REAL,
                ra REAL,
                dec REAL,
                age REAL,
                sgscore REAL,
                delta_t REAL,
                distance_arcmin REAL,
                distance_ratio REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (event_id, object_id)
            )
        ''')
    except sqlite3.OperationalError:
        print("archival_xmatches table already exists.")

    # commit the changes and close the connection
    conn.commit()
    conn.close()
    return

# third migration adds the ndethist column to the xmatches and archival_xmatches tables
def migration3():
    conn = sqlite3.connect('./data/database.db')
    c = conn.cursor()

    # add the ndethist column to the xmatches table
    try:
        c.execute('ALTER TABLE xmatches ADD COLUMN ndethist INTEGER')
    except sqlite3.OperationalError:
        print("xmatches table already has ndethist column.")
    # add the ndethist column to the archival_xmatches table
    try:
        c.execute('ALTER TABLE archival_xmatches ADD COLUMN ndethist INTEGER')
    except sqlite3.OperationalError:
        print("archival_xmatches table already has ndethist column.")

    # commit the changes and close the connection
    conn.commit()
    conn.close()
    return

# fourth migration adds the distpsnr, ssdistnr, ssmagnr to the xmatches and archival_xmatches tables
def migration4():
    conn = sqlite3.connect('./data/database.db')
    c = conn.cursor()

    # add the distpsnr, ssdistnr, ssmagnr columns to the xmatches table
    for column in ['distpsnr', 'ssdistnr', 'ssmagnr']:
        try:
            c.execute(f'ALTER TABLE xmatches ADD COLUMN {column} REAL')
        except sqlite3.OperationalError:
            print(f"xmatches table already has {column} column.")
    # add the distpsnr, ssdistnr, ssmagnr columns to the archival_xmatches table
    for column in ['distpsnr', 'last_detected_ssdistnr', 'last_detected_ssmagnr']:
        try:
            c.execute(f'ALTER TABLE archival_xmatches ADD COLUMN {column} REAL')
        except sqlite3.OperationalError:
            print(f"archival_xmatches table already has {column} column.")
    # commit the changes and close the connection
    conn.commit()
    conn.close()
    return

migrations = [
    migration1,
    migration2,
    migration3,
    migration4,
]

def run_migrations():
    prv_migrated = False
    for migration in migrations:
        try:
            migration()
            prv_migrated = True
        except Exception as e:
            print(f"Migration {migration.__name__} failed: {e}")
            if prv_migrated:
                exit(1)
    print("All migrations completed successfully.")

if __name__ == "__main__":
    run_migrations()