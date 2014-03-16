"""PostgreSQL DB helpers.

Uses two pools for PostgreSQL DB connections: one pool for connections in autocommit mode used by execute(),
one pool for connections in transaction mode.

Interface::
* autocommit() -> context manager, returns psycopg2.Cursor (in autocommit mode)
* execute(statement, params=None, repeat=True) -> psycopg2.Cursor
* transaction() -> context manager, returns psycopg2.Cursor inside explicit transaction
"""
import collections
import contextlib
# import logbook
import logging
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.pool
import random
import time


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)


try:
    import sqlalchemy
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect
    _sa_class = sqlalchemy.sql.ClauseElement
    _sa_dialect = PGDialect()
except ImportError:
    _sa_class = None
    _sa_dialect = None


POSTGRESQL_DSN = ''


# Select logbook or logging here
# log = logbook.Logger('db')
log = logging.getLogger('db')


class Error(Exception):
    pass


class DictCursor(psycopg2.extras.DictCursor):

    def execute(self, statement, params=None, record_type=None,
                _sa_class=_sa_class, _sa_dialect=_sa_dialect):
        """Psycopg2.Cursor.execute wrapped with query time logging.
        Returns self, so you can chain it with fetch* methods, etc.
        """
        if _sa_class is not None and isinstance(statement, _sa_class):
            compiled = statement.compile(dialect=_sa_dialect)
            statement, params = compiled.string, compiled.params

        self.connection.notices[:] = []
        error = None
        start = time.time()
        try:
            super(DictCursor, self).execute(statement, params)
        except psycopg2.Error as error:
            pass
        total = round(time.time() - start, 3)

        for notice in self.connection.notices:
            log.notice(notice.strip().decode('utf-8', 'replace'))
            if notice == "WARNING:  there is already a transaction in progress\n":
                raise Error(u"Nested BEGIN inside transaction. Aborting possibly broken code.")

        sql = (self.mogrify(statement, params)
               if not statement.lower().startswith("insert")
               else statement).decode('utf-8', 'replace')
        sql_id = id(sql)
        log.info(u"Query [{time:.3f}] id={id} {sql}".format(
            time=total, id=sql_id, sql=sql))

        if error is not None:
            raise error

        return self

    def executemany(self, statement, parameters):
        return super(DictCursor, self).executemany(statement, parameters)

    def callproc(self, procname, parameters):
        return super(DictCursor, self).callproc(procname, parameters)

    def scalar(self):
        row = self.fetchone()
        if row is None:
            return None
        return row[0]


class NamedTupleCursor(psycopg2.extras.NamedTupleCursor):

    EmptyRecord = collections.namedtuple("Record", ())

    def execute(self, statement, params=None, record_type=None,
                _sa_class=_sa_class, _sa_dialect=_sa_dialect):
        """Psycopg2.Cursor.execute wrapped with query time logging.
        Returns cursor object, so you can chain it with fetch* methods, etc.
        """
        if _sa_class is not None and isinstance(statement, _sa_class):
            compiled = statement.compile(dialect=_sa_dialect)
            statement, params = compiled.string, compiled.params

        self.connection.notices[:] = []
        error = None
        start = time.time()
        try:
            super(NamedTupleCursor, self).execute(statement, params)
        except psycopg2.Error as error:
            pass
        total = round(time.time() - start, 3)

        for notice in self.connection.notices:
            log.notice(notice.strip().decode('utf-8', 'replace'))
            if notice == "WARNING:  there is already a transaction in progress\n":
                raise DbError(u"Nested BEGIN inside transaction. Aborting possibly broken code.")

        sql = (self.mogrify(statement, params)
               if not statement.lower().startswith("insert")
               else statement).decode('utf-8', 'replace')
        sql_id = id(sql)
        log.info(u"Query [{time:.3f}] id={id} {sql}".format(
            time=total, id=sql_id, sql=sql))

        if error is not None:
            raise error

        self.Record = record_type
        return self

    def executemany(self, statement, parameters):
        return super(NamedTupleCursor, self).executemany(statement, parameters)

    def callproc(self, procname, parameters):
        return super(NamedTupleCursor, self).callproc(procname, parameters)

    def scalar(self):
        row = self.fetchone()
        if row is None:
            return None
        return row[0]

    def _make_nt(self, _namedtuple=collections.namedtuple):
        if not self.description:
            return NamedTupleCursor.EmptyRecord
        columns = [d[0] if d[0] != "?column?" else "column" + str(i)
                   for i, d in enumerate(self.description, 1)]
        return _namedtuple("Record", columns)


# Select default cursor class here
# default_cursor_class = DictCursor
default_cursor_class = NamedTupleCursor


class ReadCursor(object):

    """Read-only cursor-like object.
    """
    rowcount = property(lambda self: self._rowcount)

    def __init__(self, rows, rowcount):
        self._rows = rows
        self._rowcount = rowcount

    def fetchone(self):
        if self._rows is None:
            raise psycopg2.ProgrammingError("no results to fetch")
        if self._rowcount == 0:
            return None
        return self._rows[0]

    def fetchmany(self, size=None):
        if self._rows is None:
            raise psycopg2.ProgrammingError("no results to fetch")
        if size is None:
            return self._rows
        return self._rows[:size]

    def fetchall(self):
        if self._rows is None:
            raise psycopg2.ProgrammingError("no results to fetch")
        return self._rows

    def __iter__(self):
        if self._rows is None:
            raise psycopg2.ProgrammingError("no results to fetch")
        return iter(self._rows)

    def scalar(self):
        if self._rows is None:
            raise psycopg2.ProgrammingError("no results to fetch")
        if self._rowcount == 0:
            return None
        return self._rows[0][0]


class Connection(psycopg2.extensions.connection):

    def commit(self):
        start = time.time()
        super(Connection, self).commit()
        total = time.time() - start
        log.info(u"Commit [{time:.3f}]".format(time=total))

    def rollback(self):
        start = time.time()
        super(Connection, self).rollback()
        total = time.time() - start
        log.info(u"Rollback [{time:.3f}]".format(time=total))

    def cursor(self, _klass=default_cursor_class, *args, **kwargs):
        return super(Connection, self).cursor(
            *args, cursor_factory=_klass, **kwargs)


class ThreadConnectionPool(psycopg2.pool.ThreadedConnectionPool):

    @contextlib.contextmanager
    def item(self):
        close = True
        conn = self.getconn()
        # Note: makes round-trip to DB. Only required for new connections.
        conn.autocommit = True
        try:
            yield conn
            # no error
            close = False
        finally:
            self.putconn(conn, close=close or conn.closed)


def is_connection_error(e):
    """Exception object -> True | False
    """
    if not isinstance(e, psycopg2.DatabaseError):
        return False
    error_str = str(e)
    MSG1 = "socket not open"
    MSG2 = "server closed the connection unexpectedly"
    MSG3 = "could not connect to server"
    return MSG1 in error_str or MSG2 in error_str or MSG3 in error_str


# TODO: override this if necessary
def get_connection_pool(group):
    # the most straightforward threaded pool built-in psycopg2
    return ThreadConnectionPool(
        minconn=0, maxconn=10,
        dsn=POSTGRESQL_DSN, connection_factory=Connection)

    # pre initialized pools for different groups of database servers
    # return group_map[group]


@contextlib.contextmanager
def autocommit(group='default', connection_pool=None):
    """Context manager.
    Executes block with new cursor from pooled connection in autocommit mode. Returns cursor.
    At the end of the block, the connection is returned to pool.

    >>> with autocommit() as cursor:
    ...     cursor.execute("select 1")
    ...     cursor.execute("select 2")

    Use it when you do several selects and don't want to waste time for final ROLLBACK.
    """
    pool = connection_pool or get_connection_pool(group)

    with pool.item() as connection:
        cursor = connection.cursor()
        yield cursor


def execute(statement, params=None, group='default', connection_pool=None, repeat=True, record_type=None):
    """Shortcut for
    1. get connection from pool, create new cursor
    2. cursor.execute(statement, params)
    3. cursor.fetchall() (if possible)
    4. return connection to pool

    Returns read-only cursor with rows.

    On disconnect, if `repeat is True` attempts reconnect and repeats function call one more time.
    If second attempt fails, raises exception.
    """
    pool = connection_pool or get_connection_pool(group)

    with pool.item() as connection:
        try:
            cursor = connection.cursor()
            cursor.execute(statement, params, record_type=record_type)

            rows = None
            rowcount = cursor.rowcount
            try:
                rows = cursor.fetchall()
            except psycopg2.ProgrammingError as e:
                if str(e) != "no results to fetch":
                    raise
            return ReadCursor(rows, rowcount)
        except psycopg2.DatabaseError as e:
            if repeat and is_connection_error(e):
                log.warning(u"execute() DB disconnect, repeating query.")
            else:
                raise

    # Connection lost, repeat.
    return execute(statement, params, repeat=False)


def transaction(group='default', connection_pool=None):
    """Context manager.
    Executes block with new cursor from pooled connection in transaction. Returns cursor.
    At the end of the block, the connection is returned to pool.
    Transaction is commited "on success".

    >>> with transaction() as cursor:
    ...     rows = cursor.execute(...).fetchall()
    ...     process(rows)
    ...     cursor.execute(...)

    Always use it instead of manual BEGIN/ROLLBACK-s.
    """
    pool = connection_pool or get_connection_pool(group)

    with pool.item() as connection:
        cursor = connection.cursor()
        cursor.execute("begin")
        try:
            yield cursor
        except Exception:
            cursor.execute("rollback")
            raise
        else:
            cursor.execute("commit")
