import psycopg2
from psycopg2.extras import RealDictCursor


class DatabaseUtils:
    """
    Some utils to manage database conections, selects, etc.
    """

    def get_connection(self, params):
        """
        Open a connection to Postgres database.
        :param params: a dict with connection parameters: host, user, password, schema and database
        :return: the opened connection
        """
        conn_string = "host='{host}' dbname='{db_name}' user='{user}' password='{password}' options='-c search_path={schema},public'"

        conn_string = conn_string.format(
            host=params["host"],
            user=params["user"],
            password=params["password"],
            schema=params["schema"],
            db_name=params["db_name"],
        )
        connection = psycopg2.connect(conn_string)
        connection.autocommit = params['autocommit']
        return connection

    def select(self, connection, select_command):
        """
        Perform a select command.
        :param connection: a opened connection.
        :param select_command: the select command
        :return: the rows returned by the select command.
        """
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(select_command)
        rows = cursor.fetchall()
        return rows

    def execute(self, connection, sql_command):
        """
        Perform a SQL command.
        :param connection: a opened connection.
        :param select_command: the select command
        """
        cursor = connection.cursor()
        cursor.execute(sql_command)


class MoveTable:
    """
    Move all objects from a Postgres schema to another.
    """

    def execute(self, *args, **kwargs):
        params = kwargs.get('params')
        if params is None:
            raise Exception('Params not defined')
        utils = DatabaseUtils()
        kwargs['utils'] = utils
        conn = utils.get_connection(params)
        try:
            with conn:
                self._set_up(conn, *args, **kwargs)
                try:
                    kwargs['tables'] = self._get_all_tables(conn, *args, **kwargs)
                    self._move_tables(conn, *args, **kwargs)
                finally:
                    self._tear_down(conn, *args, **kwargs)
        finally:
            conn.close()

    def _set_up(self, connection, *args, **kwargs):
        pass

    def _tear_down(self, connection, *args, **kwargs):
        pass

    def _build_sql_to_move_table(self, schema_from, table_name, schema_to):
        sql = "alter table {schema_from}.{table} set schema {schema_to};".format(
            schema_from=schema_from,
            table=table_name,
            schema_to=schema_to,
        )
        return sql

    def _move_tables(self, connection, *args, **kwargs):
        schema_to = kwargs['params']['schema_to']
        all_tables = kwargs['tables']
        utils = kwargs['utils']
        for schema_from, tables in all_tables.items():
            for table_name in tables:
                sql = self._build_sql_to_move_table(schema_from, table_name, schema_to)
                utils.execute(connection, sql)

    def _build_sql_to_get_all_tables(self, schema):
        sql = "select tablename from pg_tables where schemaname = '{schema}';".format(
            schema=schema,
        )
        return sql

    def _get_all_tables(self, connection, *args, **kwargs):
        schemas_from = kwargs['params']['schemas_from']
        utils = kwargs['utils']
        result = {}
        for schema in schemas_from:
            sql = self._build_sql_to_get_all_tables(schema)
            if sql is not None:
                tables = utils.select(connection, sql)
                result[schema] = [table['tablename'] for table in tables]
        return result


class InsertRows(MoveTable):
    def _build_sql_to_move_table(self, schema_from, table_name, schema_to):
        sql = "insert into {schema_from}.{table} select * from {schema_to}.{table};".format(
            schema_from=schema_from,
            table=table_name,
            schema_to=schema_to,
        )
        return sql

