from pathlib import Path
from datetime import datetime


TERM_COLORS = {
    "black": "\033[30m",
    "red": "\033[31m",
    "green": "\033[32m",
    "orange": "\033[33m",
    "blue": "\033[34m",
    "purple": "\033[35m",
    "cyan": "\033[36m",
    "lightgrey": "\033[37m",
    "darkgrey": "\033[90m",
    "lightred": "\033[91m",
    "lightgreen": "\033[92m",
    "yellow": "\033[93m",
    "lightblue": "\033[94m",
    "pink": "\033[95m",
    "lightcyan": "\033[96m",
    "reset": "\033[00m",
}


def print_color(msg, color):
    if not color in TERM_COLORS:
        raise ValueError(f"Color {color} is not supported")
    print(TERM_COLORS[color] + msg + TERM_COLORS["reset"])


def print_error(msg):
    return print_color(msg, "red")


def print_warning(msg):
    return print_color(msg, "orange")


def print_title(msg):
    return print_color(msg, "lightgreen")


def print_info(msg):
    return print_color(msg, "yellow")


def ensure_path_exists(path, is_file=False):
    if isinstance(path, str):
        path = Path(path)
    if is_file:
        tmp = path.parent
    else:
        tmp = path
    if not tmp.exists():
        tmp.mkdir(exist_ok=True, parents=True)
    return path


def extract_infos_2019(file_path):
    infos = {}
    date, time, rec_id = file_path.stem.split("_")
    full_date = datetime.strptime(f"{date}_{time}", "%Y%m%d_%H%M%S")
    infos["full_date"] = full_date
    infos["date"] = datetime.strptime(date, "%Y%m%d")
    infos["date_hour"] = datetime.strptime(full_date.strftime("%Y%m%d_%H"), "%Y%m%d_%H")
    infos["rec_id"] = rec_id
    infos["path"] = file_path
    return infos


def extract_infos_2018(file_path):
    infos = {}
    timestamp, rec_id = file_path.stem.split("_")
    full_date = datetime.fromtimestamp(int(timestamp, 16))
    infos["full_date"] = full_date
    infos["date"] = datetime.strptime(full_date.strftime("%Y%m%d"), "%Y%m%d")
    infos["date_hour"] = datetime.strptime(full_date.strftime("%Y%m%d_%H"), "%Y%m%d_%H")
    infos["rec_id"] = rec_id
    infos["path"] = file_path
    return infos
