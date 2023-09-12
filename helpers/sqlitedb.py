import os
import sqlite3

from helpers import setup, utils

db_file = "".join([setup.get_from_settings("db_name"), ".db"])
sql_dir = "sql"
table_scripts = os.path.join(sql_dir, "tables")


def start_query():
    try:
        return sqlite3.connect(db_file)
    except Exception as e:
        raise Exception(f"Failed to create db connection: {e}")


def end_query(conn_to_close):
    try:
        conn_to_close.close()
    except Exception as e:
        raise Exception(f"Failed to close db connection: {conn_to_close} {e}")


def create_db():
    try:
        conn = start_query()
        cursor = conn.cursor()
        for query in utils.get_creation_scripts():
            cursor.execute(query)
            conn.commit()
        cursor.close()
        end_query(conn)
    except Exception as e:
        raise Exception(f"Could not create db and tables: {e}")


def insert_many(script_name: str, records: list):
    try:
        conn = start_query()
        cursor = conn.cursor()

        query = utils.get_change_script(script_name)
        cursor.executemany(query, records)
        conn.commit()

        print("Total", cursor.rowcount, f"records inserted: {script_name}")
        cursor.close()
        end_query(conn)
    except sqlite3.IntegrityError as ie:
        print(f"Integrity error on insert: {script_name} {records} {ie}")
    except Exception as e:
        raise Exception(f"Could not insert records: {script_name} {records} {e}")


# def get_results_from_val(script_name: str, param: str) -> list:
#     try:
#         conn = start_query()
#         cursor = conn.cursor()

#         query = utils.get_query_script(script_name)
#         cursor.execute(query, (param,))
#         matches = cursor.fetchall()

#         cursor.close()
#         end_query(conn)
#         return matches
#     except Exception as e:
#         raise Exception(f"Could not query database: {script_name} {param} {e}")


def get_all_results(script_name: str) -> list:
    try:
        conn = start_query()
        cursor = conn.cursor()

        query = utils.get_query_script(script_name)
        cursor.execute(query)
        matches = cursor.fetchall()

        cursor.close()
        end_query(conn)
        return matches
    except Exception as e:
        raise Exception(f"Could not query database: {script_name} {e}")


def execute_query(script_name: str):
    try:
        conn = start_query()
        cursor = conn.cursor()

        query = utils.get_change_script(script_name)

        cursor.execute(query)
        conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")

        cursor.close()
        end_query(conn)
    except Exception as e:
        raise Exception(f"Could not execute query: {script_name} {e}")


def batch_query(script: str):
    conn = start_query()
    for records in get_batched_results(script, conn):
        yield records
    end_query(conn)


def batch_query_from_list(script: str, vals: list):
    conn = start_query()
    for records in get_batched_results_from_list(script, vals, conn):
        yield records
    end_query(conn)


def batch_updates(script: str):
    conn = start_query()
    for records in get_batched_results(script, conn):
        yield (records, conn)
    end_query(conn)


def batch_updates_from_list(script: str, vals: list):
    conn = start_query()
    for records in get_batched_results_from_list(script, vals, conn):
        yield (records, conn)
    end_query(conn)


def get_batched_results(script_name: str, db_conn, batch_size: int = 300):
    try:
        cursor = db_conn.cursor()

        query = utils.get_query_script(script_name)
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


def get_batched_results_from_list(
    script_name: str, records: list, db_conn, batch_size: int = 300
):
    try:
        cursor = db_conn.cursor()

        list_placeholders = ",".join(["?"] * len(records))
        query = utils.get_query_script(script_name) % list_placeholders
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


def insert_during_batch(script_name: str, records: list, db_conn):
    try:
        cursor = db_conn.cursor()

        query = utils.get_change_script(script_name)
        cursor.executemany(query, records)
        db_conn.commit()

        print("Total", cursor.rowcount, f"records inserted: {script_name}")
        cursor.close()
    except sqlite3.IntegrityError as ie:
        print(f"Integrity error on insert: {script_name} {records} {ie}")
    except Exception as e:
        raise Exception(f"Could not insert records: {script_name} {records} {e}")


def execute_list_during_batch(script_name: str, records: list, db_conn):
    try:
        cursor = db_conn.cursor()

        list_placeholders = ",".join(["?"] * len(records))
        query = utils.get_change_script(script_name) % list_placeholders

        cursor.execute(query, records)
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not execute query with list: {script_name} {records} {e}"
        )


def execute_many_during_batch(script_name: str, records: list, db_conn):
    try:
        cursor = db_conn.cursor()

        query = utils.get_change_script(script_name)

        cursor.executemany(query, records)
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not execute query with values: {script_name} {records} {e}"
        )


def execute_with_val_during_batch(script_name: str, param: str, db_conn):
    try:
        cursor = db_conn.cursor()

        query = utils.get_change_script(script_name)

        cursor.execute(query, (param,))
        db_conn.commit()
        print("Total", cursor.rowcount, f"records impacted by script: {script_name}")
        cursor.close()
    except Exception as e:
        raise Exception(
            f"Could not execute query with values: {script_name} {param} {e}"
        )
