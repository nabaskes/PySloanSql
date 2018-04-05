import requests
import abc
from cursor import Cursor


class Connection(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, data_release=None, read_timeout=None, cursorclass=Cursor):
        self._data_release = data_release
        self.read_timeout = read_timeout
        self._cursorclass = cursorclass

    def cursor(self, cursorclass=None):
        if cursorclass:
            self.cursor_class = cursorclass
        return self._cursorclass(self)

    @property
    def cursor_class(self):
        return self._cursorclass



class DR14Connection(Connection):
    url = "http://skyserver.sdss.org/dr14/SkyServerWS/SearchTools/SqlSearch?cmd=%s&format=csv"

    def __init__(self, data_release=None, read_timeout=None, cursorclass=Cursor):
        super().__init__(data_release=data_release, read_timeout=read_timeout, cursorclass=cursorclass)

    def query(self, q):
        self._execute_command(q)
        self._affected_rows = self._read_query_result()
        return self._affected_rows

    def next_result(self):
        self._affected_rows = self._read_query_result()
        return self._affected_rows

    def affected_rows(self):
        return self._affected_rows

    def ping(self):
        raise NotImplementedError("This is easy to do but not important")


    def _read_query_result(self):
        result = SQLResult(self)
        result.init_query()
        return result.affected_rows


class SQLResult(object):
    def __init__(self, connection):
        self.connection = connection
        self.affected_rows = None
        self.insert_id = None
        self.warning_count = None
        self.message = None
        self.has_next = None
        self.rows = None
