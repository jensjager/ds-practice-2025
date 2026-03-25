import logging
import os
import sys


def setup_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def add_grpc_path(current_file, relative_path):
    base_file = current_file if current_file else os.getenv("PYTHONFILE", "")
    grpc_path = os.path.abspath(os.path.join(base_file, relative_path))
    if grpc_path not in sys.path:
        sys.path.insert(0, grpc_path)


def env_int(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default
