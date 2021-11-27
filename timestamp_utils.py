"""Code that manipulates timestamps"""
import datetime

from constants import TIMESTAMP_COL


def convert_timestamp(message):
    # assuming that message is already parsed JSON (dict)
    return datetime.datetime.strptime(message, "%Y-%m-%d %H:%M:%S UTC")


def filter_timestamps(row, year):
    date = convert_timestamp(row[TIMESTAMP_COL])
    return date.year >= year
