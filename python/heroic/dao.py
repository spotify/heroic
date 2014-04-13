import itertools
import logging

from heroic.models import RowKey

log = logging.getLogger(__name__)

DEFAULT_LIMIT = 1000

LIST_KEYS_STMT = (
    "SELECT DISTINCT key FROM data_points "
    "WHERE token(key) > token(?) "
    "LIMIT ?")

DELETE_DATA_POINTS = "DELETE FROM data_points WHERE key = ?"
DELETE_ROW_KEY_INDEX = (
    "DELETE FROM row_key_index "
    "WHERE key=? AND column1 = ?")


class DAO(object):
    def __init__(self, session):
        self.session = session

    def list_keys(self, limit=DEFAULT_LIMIT):
        last = ""

        for i in itertools.count():
            if last is None:
                break

            start = i * limit
            stop = (i + 1) * limit

            log.debug("Scanning from: {} - {}".format(start, stop))

            stmt = self.session.prepare(LIST_KEYS_STMT).bind((last, limit))

            result = self.session.execute(stmt, timeout=None)

            last = None

            for row in result:
                yield row, RowKey.deserialize(row.key)
                last = row.key

    def delete_timeseries(self, key, force_non_buggy=False):
        row_key = RowKey.deserialize(key)

        if not force_non_buggy and not row_key.is_buggy():
            raise Exception(
                "Refusing to delete non-buggy timeseries: {}".format(
                    repr(key)))

        row_key_index_stmt = self.session.prepare(
            DELETE_ROW_KEY_INDEX).bind((row_key.key, key))
        data_points_stmt = self.session.prepare(
            DELETE_DATA_POINTS).bind((key,))

        result0 = self.session.execute(row_key_index_stmt, timeout=None)
        result1 = self.session.execute(data_points_stmt, timeout=None)
        return result0, result1
