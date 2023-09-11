import yaml


settings_file = "./settings.yaml"

def get_from_config(setting_key: str):
    try:
        with open(settings_file, "r") as f:
            settings = yaml.safe_load(f)
            setting = settings.get(setting_key)

            return setting
    except Exception as e:
        raise Exception(f"Could not process settings: {e}")


def get_exiftool_extensions():
    try:
        with open("config/exif.yaml", "r") as f:
            extensions = yaml.safe_load(f)
            extensions = extensions.get("extension")
            return extensions
    except Exception as e:
        raise Exception(f"Could not get list of extentions: {e}")