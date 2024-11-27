import json
from pathlib import Path


def get_config() -> tuple[str, int]:
    """Loads and returns TEMP_DIR and CHUNK_SIZE from a JSON file.

    Reads the configuration data from 'etl_assignment/config/config.json'
    file and returns the values as a list.

    Returns:
        list: Values TEMP_DIR (local storage) and CHUNK_ZISE (parsing size).
    """
    config_file = Path("etl_assignment/config/config.json")
    with open(config_file) as json_file:
        config = json.load(json_file)
    return tuple([v for v in config.values()])
