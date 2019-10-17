import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import time


class Utils:
    @classmethod
    def print_message(cls, message):
        print("[", datetime.now().strftime("%H:%M:%S.%f"), "]", message)

    @classmethod
    def to_hour_minute_second(cls, seconds):
        """
        from https://www.geeksforgeeks.org/python-program-to-convert-seconds-into-hours-minutes-and-seconds/
        """
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        return "%d:%02d:%02d" % (hour, minutes, seconds)


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
        start_time = time.time()
        params = kwargs.get('params')
        if params is None:
            raise Exception('Params not defined')
        utils = DatabaseUtils()
        kwargs['utils'] = utils
        conn = utils.get_connection(params)
        try:
            with conn:
                schemas_from = kwargs['params']['schemas_from']
                schema_to = kwargs['params']['schema_to']
                kwargs['schemas'] = [schema_to]
                kwargs['rows'] = self._get_all_tables(conn, *args, **kwargs)
                self.set_up(conn, *args, **kwargs)
                try:
                    Utils.print_message("Getting table")
                    kwargs['schemas'] = schemas_from
                    kwargs['tables'] = self._get_all_tables(conn, *args, **kwargs)
                    Utils.print_message("Moving table")
                    self._move_tables(conn, *args, **kwargs)
                    exclude_schemas_from = kwargs['params'].get('exclude_schemas_from') or False
                    if exclude_schemas_from:
                        Utils.print_message("Removing schemas")
                        self._exclude_schemas(conn, *args, **kwargs)
                finally:
                    self.tear_down(conn, *args, **kwargs)
        finally:
            conn.close()

        seconds = round(time.time() - start_time, 2)
        duration = Utils.to_hour_minute_second(seconds)
        print("------")
        print("FINISH")
        print("------")
        print("--- %s DURATION ---" % duration)
        print("------")

    def _build_sql_to_remove_schema(self, schema):
        sql = "drop schema if exists {schema} cascade;".format(
            schema=schema,
        )
        return sql

    def _exclude_schemas(self, connection, *args, **kwargs):
        utils = kwargs['utils']
        schemas = kwargs['params']['schemas_from']
        for schema in schemas:
            Utils.print_message("...removing" + schema)
            sql = self._build_sql_to_remove_schema(schema)
            if sql is not None:
                utils.execute(connection, sql)

    def _build_sql_to_enable_trigger(self, table_name, action, restrict):
        sql = "alter table if exists {table_name} {action} trigger {restrict};".format(
            table_name=table_name,
            action=action,
            restrict=restrict,
        )
        return sql

    def _enable_trigger(self, connection, *args, **kwargs):
        utils = kwargs['utils']
        action = kwargs['action']
        data = kwargs['rows']
        for table_schema, tables in data.items():
            for table_name in tables:
                table_name = self._build_table_name(table_schema, table_name)

                Utils.print_message("..." + action + " trigger, if exists, on " + table_name)

                sql = self._build_sql_to_enable_trigger(table_name, action, restrict='all')
                if sql is None:
                    return
                try:
                    utils.execute(connection, sql)
                    continue
                except:
                    pass
                sql = self._build_sql_to_enable_trigger(table_name, action, restrict='user')
                if sql is not None:
                    utils.execute(connection, sql)

    def set_up(self, connection, *args, **kwargs):
        kwargs['action'] = 'disable'
        Utils.print_message("Disabling trigger")
        self._enable_trigger(connection, *args, **kwargs)

    def tear_down(self, connection, *args, **kwargs):
        kwargs['action'] = 'enable'
        Utils.print_message("Enabling trigger")
        self._enable_trigger(connection, *args, **kwargs)

    def _build_table_name(self, schema_name, table_name):
        return '%s.%s' % (schema_name, table_name)

    def _build_sql_to_move_table(self, schema_from, table_name, columns, schema_to):
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
            for table_name, columns in tables.items():
                Utils.print_message("...moving " + schema_from + "." + table_name + " => " + schema_to)
                sql = self._build_sql_to_move_table(schema_from, table_name, columns, schema_to)
                if sql is not None:
                    utils.execute(connection, sql)

    def _build_sql_to_get_all_tables(self, schema, except_tables):
        if except_tables is None:
            columns = 'null'
        else:
            columns = ",".join(["'" + table + "'" for table in except_tables])
        sql = """
        select table_schema, table_name, column_name 
        from information_schema.columns 
        where table_schema = '{schema}' 
        and is_updatable = 'YES'
        and table_name not in ({except_tables})
        order by table_schema, table_name, ordinal_position;
        """.format(
            schema=schema,
            except_tables=columns
        )
        return sql

    def _get_all_tables(self, connection, *args, **kwargs):
        schemas = kwargs['schemas']
        utils = kwargs['utils']
        except_tables = kwargs['params'].get('except_tables')
        result = {}
        for schema in schemas:
            sql = self._build_sql_to_get_all_tables(schema, except_tables)
            if sql is not None:
                tables = utils.select(connection, sql)
                for table in tables:
                    table_schema = table['table_schema']
                    if not result.get(table_schema):
                        result[table_schema] = {}
                    table_name = table['table_name']
                    if not result[table_schema].get(table_name):
                        result[table_schema][table_name] = []
                    column_name = table['column_name']
                    result[table_schema][table_name].append(column_name)
        return result


class InsertRows(MoveTable):
    def _build_sql_to_move_table(self, schema_from, table_name, columns, schema_to):
        sql = "insert into {schema_to}.{table}({columns}) select {columns} from {schema_from}.{table};".format(
            schema_from=schema_from,
            table=table_name,
            schema_to=schema_to,
            columns=",".join(columns)
        )
        return sql

