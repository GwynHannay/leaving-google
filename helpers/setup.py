import os
import yaml


SETTINGS_FILE = "./settings.yaml"
CONFIG_FILE = "config/program.yaml"
SQL_DIR = "sql"


def get_from_settings(setting_key: str):
    try:
        with open(SETTINGS_FILE, "r") as f:
            settings = yaml.safe_load(f)
            setting = settings.get(setting_key)

            return setting
    except Exception as e:
        raise Exception(f"Could not get settings item: {setting_key} {e}")


def get_from_config(config_key: str):
    try:
        with open(CONFIG_FILE, "r") as f:
            config = yaml.safe_load(f)
            config = config.get(config_key)
            return config
    except Exception as e:
        raise Exception(f"Could not get config item: {config_key} {e}")


def get_queries_folder() -> str:
    folder = os.path.join(SQL_DIR, "queries")
    return folder


def get_changes_folder() -> str:
    folder = os.path.join(SQL_DIR, "changes")
    return folder


def get_creation_folder() -> str:
    folder = os.path.join(SQL_DIR, "tables")
    return folder


def get_db() -> str:
    try:
        db_name = "".join([get_from_settings("db_name"), ".db"])
        return db_name
    except Exception as e:
        raise Exception(f"Could not get database name: {e}")


def get_extensions_list() -> list:
    try:
        extensions = [
            "".join([".", ext]).lower() for ext in get_from_config("extensions")
        ]
        return extensions
    except Exception as e:
        raise Exception(f"Could not get list of extensions: {e}")
