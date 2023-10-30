import sqlite3
from helpers import sqlitedb, utils, filesystem


def create_db():
    try:
        conn = sqlitedb.open_db()
        cursor = conn.cursor()
        for query in filesystem.get_creation_scripts():
            cursor.execute(query)
            conn.commit()

        cursor.close()
        sqlitedb.close_db(conn)
    except Exception as e:
        raise Exception(f"Could not create db and tables: {e}")


def insert_many(script_name: str, records: list, db_conn=None):
    conn, query = sqlitedb.begin_change(script_name, db_conn)
    state, result = sqlitedb.execute_with_many_values(query, records, conn)

    if isinstance(result, sqlite3.Cursor):
        msg = f"Total {result.rowcount} records inserted: {script_name}"
    else:
        msg = f"Inserting records failed: {script_name}, {records}"

    if db_conn:
        conn = 0

    sqlitedb.finish_query(state, (result, conn), msg)


def insert_row(script_name: str, record: tuple) -> int:
    conn, query = sqlitedb.begin_change(script_name)
    state, result = sqlitedb.execute_with_values(query, record, conn)

    row_id = None
    msg = None
    if isinstance(result, sqlite3.Cursor):
        row_id = result.lastrowid
    else:
        msg = f"Inserting row failed: {script_name}, {record}, {result}"

    if not row_id:
        row_id = 0

    sqlitedb.finish_query(state, (result, conn), msg)
    return row_id


def run_query(script_name: str):
    conn, query = sqlitedb.begin_change(script_name)
    state, result = sqlitedb.execute_without_values(query, conn)

    if isinstance(result, sqlite3.Cursor):
        msg = f"Total {result.rowcount} records impacted: {script_name}"
    else:
        msg = f"Query failed: {script_name}, {result}"

    sqlitedb.finish_query(state, (result, conn), msg)


def update_with_list(script_name: str, records: list, db_conn=None):
    conn, raw_query = sqlitedb.begin_change(script_name, db_conn)
    query = utils.prepare_list_query(raw_query, records)
    state, result = sqlitedb.execute_with_values(query, records, conn)

    if isinstance(result, sqlite3.Cursor):
        msg = f"Total {result.rowcount} records impacted: {script_name}"
    else:
        msg = f"Update with list failed: {script_name}, {records}, {result}"

    if db_conn:
        conn = 0

    sqlitedb.finish_query(state, (result, conn), msg)


def update_with_many_vals(script_name: str, records: list, db_conn=None):
    conn, query = sqlitedb.begin_change(script_name, db_conn)
    state, result = sqlitedb.execute_with_many_values(query, records, conn)

    if isinstance(result, sqlite3.Cursor):
        msg = f"Total {result.rowcount} records impacted: {script_name}"
    else:
        msg = f"Update failed: {script_name}, {records}, {result}"

    if db_conn:
        conn = 0

    sqlitedb.finish_query(state, (result, conn), msg)


def update_with_single_val(script_name: str, param: str, db_conn=None):
    conn, query = sqlitedb.begin_change(script_name, db_conn)
    state, result = sqlitedb.execute_with_values(query, (param,), conn)

    if isinstance(result, sqlite3.Cursor):
        msg = f"Total {result.rowcount} records impacted: {script_name}"
    else:
        msg = f"Update failed: {script_name}, {param}, {result}"

    if db_conn:
        conn = 0

    sqlitedb.finish_query(state, (result, conn), msg)


def get_records(script_name: str, params=None, batch_size: int = 300):
    conn, query = sqlitedb.begin_query(script_name)

    if params:
        state, result = sqlitedb.execute_with_many_values(query, params, conn)
    else:
        state, result = sqlitedb.execute_without_values(query, conn)

    if not isinstance(result, sqlite3.Cursor):
        raise Exception(f"Could not run query: {script_name}, {params}, {result}")

    while True:
        matches = result.fetchmany(batch_size)
        if matches:
            yield (matches, conn)
        else:
            break

    sqlitedb.finish_query(state, (result, conn))


def get_records_with_list(script_name: str, params: list, batch_size: int = 300):
    conn, raw_query = sqlitedb.begin_query(script_name)
    query = utils.prepare_list_query(raw_query, params)
    state, result = sqlitedb.execute_with_values(query, params, conn)

    if not isinstance(result, sqlite3.Cursor):
        raise Exception(f"Could not run query: {script_name}, {params}, {result}")

    while True:
        matches = result.fetchmany(batch_size)
        if matches:
            yield (matches, conn)
        else:
            break

    sqlitedb.finish_query(state, (result, conn))


def begin_batch_query(script: str, vals=None):
    for records, conn in get_records(script, params=vals):
        yield records


def begin_batch_query_with_list(script: str, vals: list):
    for records, conn in get_records_with_list(script, vals):
        yield records


def begin_batch_updates(script: str, vals=None):
    for records in get_records(script, params=vals):
        yield records
