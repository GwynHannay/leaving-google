import os
import sqlite3

from helpers import setup

db_file = "".join([setup.get_from_config("db_name"), ".db"])

sql_dir = "sql"
table_scripts = os.path.join(sql_dir, "tables")
queries = os.path.join(sql_dir, "queries")
changes = os.path.join(sql_dir, "changes")


def get_query_script(filename: str) -> str:
    filepath = os.path.join(queries, filename)
    query = read_sql_file(filepath)
    return query


def get_change_script(filename: str) -> str:
    filepath = os.path.join(changes, filename)
    query = read_sql_file(filepath)
    return query


def read_sql_file(sql_file: str) -> str:
    query = str
    if not sql_file.endswith(".sql"):
        sql_file = "".join([sql_file, ".sql"])

    with open(sql_file) as s:
        query = s.read()
    return query


def start_query(existing_conn = None):
    try:
        if existing_conn:
            return existing_conn
        else:
            conn = sqlite3.connect(db_file)
            return conn
    except Exception as e:
        raise Exception(f"Failed to check db connection: {existing_conn} {e}")


def end_query(conn_to_close = None):
    try:
        if conn_to_close:
            conn_to_close.close()
    except Exception as e:
        raise Exception(f"Failed to close db connection: {conn_to_close} {e}")


def create_db(db_conn):
    try:
        cursor = db_conn.cursor()
        for script in os.listdir(table_scripts):
            query = read_sql_file(os.path.join(sql_dir, "tables", script))
            cursor.execute(query)
            db_conn.commit()
        cursor.close()
    except Exception as e:
        raise Exception(f"Could not create db and tables: {e}")


def insert_many_records(script_name: str, records: list, db_conn):
    try:
        cursor = db_conn.cursor()

        query = get_change_script(script_name)
        cursor.executemany(query, records)
        db_conn.commit()

        print("Total", cursor.rowcount, f"records inserted: {script_name}")
        cursor.close()
    except sqlite3.IntegrityError as ie:
        print(f"Integrity error on insert: {script_name} {records} {ie}")
    except Exception as e:
        raise Exception(f"Could not insert records: {script_name} {records} {e}")


def get_batched_results_from_list(
    script_name: str, records: list, db_conn, batch_size: int = 300
):
    try:
        cursor = db_conn.cursor()

        list_placeholders = ",".join(["?"] * len(records))
        query = get_query_script(script_name) % list_placeholders
        batch = cursor.execute(query, records)

        while True:
            matches = batch.fetchmany(batch_size)
            if matches:
                yield matches
            else:
                break
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not query database with list: {script_name} {records} {e}"
        )


def get_batched_results(script_name: str, db_conn, batch_size: int = 300):
    try:
        cursor = db_conn.cursor()

        query = get_query_script(script_name)
        batch = cursor.execute(query)

        while True:
            matches = batch.fetchmany(batch_size)
            if matches:
                yield matches
            else:
                break
        cursor.close()
    except Exception as e:
        raise Exception(f"Could not query database: {script_name} {e}")


def get_results_with_val(script_name: str, param: tuple, db_conn) -> list:
    try:
        cursor = db_conn.cursor()

        query = get_query_script(script_name)
        cursor.execute(query, param)
        matches = cursor.fetchall()
        cursor.close()
        return matches
    except Exception as e:
        raise Exception(f"Could not query database: {script_name} {param} {e}")


def get_all_results(script_name: str, db_conn) -> list:
    try:
        cursor = db_conn.cursor()

        query = get_query_script(script_name)
        cursor.execute(query)
        matches = cursor.fetchall()
        cursor.close()
        return matches
    except Exception as e:
        raise Exception(f"Could not query database: {script_name} {e}")


def execute_query(script_name: str, db_conn):
    try:
        cursor = db_conn.cursor()

        query = get_change_script(script_name)

        cursor.execute(query)
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(f"Could not execute query: {script_name} {e}")


def execute_query_with_list(script_name: str, records: list, db_conn):
    try:
        cursor = db_conn.cursor()

        list_placeholders = ",".join(["?"] * len(records))
        query = get_change_script(script_name) % list_placeholders

        cursor.execute(query, records)
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not execute query with list: {script_name} {records} {e}"
        )


def execute_many_with_many_vals(script_name: str, records: list, db_conn):
    try:
        cursor = db_conn.cursor()

        query = get_change_script(script_name)

        cursor.executemany(query, records)
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not execute query with values: {script_name} {records} {e}"
        )


def execute_query_with_single_val(script_name: str, param: tuple, db_conn):
    try:
        cursor = db_conn.cursor()

        query = get_change_script(script_name)

        cursor.execute(query, param)
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not execute query with values: {script_name} {param} {e}"
        )
