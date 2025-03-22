import json
import sqlite3

DB_PATH = "db/database.db"

def save(alias, context, topic, key, schema, message):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO alias (alias, context, topic, key)
        VALUES (?, ?, ?, ?)
    ''', (alias, context, topic, key))

    cursor.execute('''
        INSERT OR REPLACE INTO schema (alias, schema, message)
        VALUES (?, ?, ?)
    ''', (alias, schema, json.dumps(message)))

    conn.commit()
    conn.close()


def save_context(
        CONTEXT,
        KAFKA_SCHEMA_REGISTRY_URL,
        KAFKA_SCHEMA_REGISTRY_API_KEY,
        KAFKA_SCHEMA_REGISTRY_API_SECRET,
        KAFKA_API_KEY,
        KAFKA_API_SECRET,
        KAFKA_BOOTSTRAP_SERVER,
        KAFKA_AUTH_MODULE,
):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO contexts (context, KAFKA_SCHEMA_REGISTRY_URL, KAFKA_SCHEMA_REGISTRY_API_KEY, 
        KAFKA_SCHEMA_REGISTRY_API_SECRET, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_BOOTSTRAP_SERVER, KAFKA_AUTH_MODULE)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (CONTEXT, KAFKA_SCHEMA_REGISTRY_URL, KAFKA_SCHEMA_REGISTRY_API_KEY, KAFKA_SCHEMA_REGISTRY_API_SECRET,
          KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_BOOTSTRAP_SERVER, KAFKA_AUTH_MODULE))

    conn.commit()
    conn.close()


def get_schema():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT alias, schema, message FROM schema')
    rows = cursor.fetchall()

    data = {row[0]: {"schema": row[1], "message": json.loads(row[2])} for row in rows}

    conn.close()
    return data


def get_contexts():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM contexts')
    rows = cursor.fetchall()

    contexts = {row[0]: {"KAFKA_SCHEMA_REGISTRY_URL": row[1], "KAFKA_SCHEMA_REGISTRY_API_KEY": row[2],
                         "KAFKA_SCHEMA_REGISTRY_API_SECRET": row[3], "KAFKA_API_KEY": row[4],
                         "KAFKA_API_SECRET": row[5], "KAFKA_BOOTSTRAP_SERVER": row[6], "KAFKA_AUTH_MODULE": row[7]}
                for row in rows}

    conn.close()
    return contexts

def get():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM alias')
    rows = cursor.fetchall()

    alias = {row[0]: {"alias": row[0], "context": row[1], "topic": row[2], "key": row[3]} for row in rows}

    conn.close()
    return alias

def initialize_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS alias (
            alias TEXT PRIMARY KEY,
            context TEXT,
            topic TEXT,
            key TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schema (
            alias TEXT PRIMARY KEY,
            schema TEXT,
            message TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS contexts (
            context TEXT PRIMARY KEY,
            KAFKA_SCHEMA_REGISTRY_URL TEXT,
            KAFKA_SCHEMA_REGISTRY_API_KEY TEXT,
            KAFKA_SCHEMA_REGISTRY_API_SECRET TEXT,
            KAFKA_API_KEY TEXT,
            KAFKA_API_SECRET TEXT,
            KAFKA_BOOTSTRAP_SERVER TEXT,
            KAFKA_AUTH_MODULE TEXT
        )
    ''')

    cursor.execute('''
        INSERT OR REPLACE INTO contexts (context, KAFKA_SCHEMA_REGISTRY_URL, KAFKA_SCHEMA_REGISTRY_API_KEY,
        KAFKA_SCHEMA_REGISTRY_API_SECRET, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_BOOTSTRAP_SERVER, KAFKA_AUTH_MODULE)
        VALUES ('context01', 'http://schema-registry:8081', null, null, null, null, 'kafka:9092', 'PLAINTEXT')
    ''')

    conn.commit()
    conn.close()
