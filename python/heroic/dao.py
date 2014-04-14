import itertools
import logging

from heroic.models import RowKey

class String:
    @classmethod
    def deserialize(self, s):
        return s


log = logging.getLogger(__name__)

DEFAULT_LIMIT = 1000

LIST_DATA_POINT_ROW_KEYS = (
    "SELECT DISTINCT key FROM data_points "
    "WHERE token(key) > token(?) "
    "LIMIT ?")
SELECT_ROW_KEY_INDEX_KEYS = (
    "SELECT DISTINCT key FROM row_key_index "
    "WHERE token(key) > token(?) "
    "LIMIT ?")
SELECT_ROW_KEY_INDEX_COLUMN_KEYS = (
    "SELECT column1 FROM row_key_index "
    "WHERE key = ? AND column1 > ? "
    "LIMIT ?")

DELETE_DATA_POINTS = "DELETE FROM data_points WHERE key = ?"
DELETE_ROW_KEY_INDEX_COLUMN = (
    "DELETE FROM row_key_index "
    "WHERE key = ? AND column1 = ?")


class DAO(object):
    def __init__(self, session):
        self.session = session

    def _paginate(self, stmt, serializer, bind_prefix=tuple(),
                       limit=DEFAULT_LIMIT, attr="key"):
        last = ""

        for i in itertools.count():
            if last is None:
                break

            bind = bind_prefix + (last, limit)
            execute_stmt = self.session.prepare(stmt).bind(bind)

            result = self.session.execute(execute_stmt, timeout=None)

            last = None

            for row in result:
                value = getattr(row, attr)
                yield row, serializer.deserialize(value)
                last = value

    def list_row_key_index_keys(self, limit=DEFAULT_LIMIT):
        for _, s in self._paginate(
            SELECT_ROW_KEY_INDEX_KEYS, String, limit=limit):
            for row, key in self._paginate(
                SELECT_ROW_KEY_INDEX_COLUMN_KEYS,
                RowKey, bind_prefix=(s,), limit=limit, attr="column1"):
                yield row.column1, key

    def list_data_point_row_keys(self, **kw):
        for row, row_key in self._paginate(
            LIST_DATA_POINT_ROW_KEYS, RowKey, **kw):
            yield row.key, row_key

    def delete_data_points(self, row_key, force_non_buggy=False):
        if not force_non_buggy and not row_key.is_buggy():
            raise Exception(
                "Refusing to delete non-buggy timeseries: {}".format(
                    repr(row_key)))

        data_points_stmt = self.session.prepare(
            DELETE_DATA_POINTS).bind((row_key.raw,))
        self.session.execute(data_points_stmt, timeout=None)

    def delete_row_key_index(self, row_key, force_non_buggy=False):
        if not force_non_buggy and not row_key.is_buggy():
            raise Exception(
                "Refusing to delete non-buggy timeseries: {}".format(
                    repr(row_key)))

        row_key_index_stmt = self.session.prepare(
            DELETE_ROW_KEY_INDEX_COLUMN).bind((row_key.key, row_key.raw))
        self.session.execute(row_key_index_stmt, timeout=None)
