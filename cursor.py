from errors import ProgrammingError


class Cursor(object):
    def __init__(self, connection):
        # connection is mostly just which dr you are using
        self.conn = connection
        self.url = connection.url
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None
        self._warnings_handled = None

    def close(self):
        conn = self.connection
        if conn is None:
            return
        try:
            while self.nextset():
                pass
        finally:
            self.connection = None

    def _get_db(self):
        if not self.connection:
            raise ProgrammingError("No connection associated with this db")
        return self.connection

    def _check_executed(self):
        if not self._executed:
            raise ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def _nextset(self, unbuffered=False):
        raise NotImplementedError

    def mogrify(self, query):
        'eventually, this needs to deal with getting proper typing, etc'
        return query

    def execute(self, query, args=None):
        while self.nextset():
            pass

        query = self.mogrify(query, args)

        result = self._query(query)
        self._executed = query
        return result

    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        conn.query(q)
        self._do_get_result()
        return self.rowcount

    def _do_get_result(self):
        conn = self._get_db()

        self.rownumber = 0
        self._result = result = conn._result

        self.rowcount = result.affected_rows
        self.description = result.description
        self.lastrowid = result.insert_id
        self._rows = result.rows
        self._warnings_handled = False

        if not self._defer_warnings:
            self._show_warnings()

    def _show_warnings(self):
        if self._warnings_handled:
            return
        self._warnings_handled = True
        if self._result and (self._result_has_next or not self._result.warning_count):
            return
        ws = self._get_db()._show_warning()
        if ws is None:
            return
        for w in ws:
            msg = w[-1]
            print(msg)
        raise NotImplementedError("_show_warnings isn't yet implemented fully")

    def fetchone(self):
        "Fetch the next row"
        self._check_executed()
        if self._rows is None or self.rownumber >= len(self._rows):
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        "Fetch several rows"
        self._check_executed()
        if self._rows is None:
            return ()
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        "Fetches all the rows"
        self._check_executed()
        if self._rows is None:
            return ()
        if self.rownumber:
            result = self._rows[self.rownumber:]
        else:
            result = self._rows
        self.rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        self._check_executed()
        if mode == "relative":
            r = self.rownumber + value
        elif mode == "absolute":
            r = value
        if not (0 <= r < len(self._rows)):
            raise IndexError("Scroll index out of range")
        self.rownumber = r

    def __iter__(self):
        return iter(self.fetchone, None)


class DictCursorMixin(object):
    dict_type = dict # override this if you want results as say a set, pandas df, numpy array, user defined object, etc

    def _do_get_result(self):
        super(DictCursorMixin, self)._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_rows(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class DictCursor(DictCursorMixin, Cursor):
    "A cursor which returns results as a dictionary"
