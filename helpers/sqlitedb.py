import sqlite3
from helpers import filesystem, setup

SUCCESS = "Succeeded"
FAILURE = "Failed"


def open_db():
    try:
        return sqlite3.connect(setup.get_db())
    except Exception as e:
        raise Exception(f"Failed to create db connection: {e}")


def close_db(conn_to_close, cursor=None):
    try:
        if cursor:
            cursor.close()
        conn_to_close.close()
    except Exception as e:
        raise Exception(f"Failed to close db connection: {conn_to_close} {e}")


def begin_change(script_name: str, db_conn=None):
    if db_conn:
        return (db_conn, filesystem.get_change_script(script_name))
    else:
        return (open_db(), filesystem.get_change_script(script_name))


def begin_query(script_name: str):
    return (open_db(), filesystem.get_query_script(script_name))


def finish_query(state: str, conn: tuple, msg=None):
    if state == SUCCESS and isinstance(conn[0], sqlite3.Cursor):
        if msg:
            print(msg)
        if isinstance(conn[1], sqlite3.Connection):
            close_db(conn[1], conn[0])
        else:
            conn[0].close()
    elif state == FAILURE:
        raise Exception("msg")
    else:
        raise Exception(f"Unexpected output: {msg}")


def execute_with_values(query: str, values, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.execute(query, values)
        db_conn.commit()

        return (SUCCESS, cursor)
    except Exception as e:
        return (FAILURE, e)


def execute_with_many_values(query: str, values, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.executemany(query, values)
        db_conn.commit()

        return (SUCCESS, cursor)
    except Exception as e:
        return (FAILURE, e)


def execute_without_values(query: str, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.execute(query)
        db_conn.commit()

        return (SUCCESS, cursor)
    except Exception as e:
        return (FAILURE, e)
