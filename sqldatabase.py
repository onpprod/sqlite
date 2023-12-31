import sqlite3
import json
from datetime import datetime


class SQLiteDB:
    """
    SQLITE by ONPPROD
    """

    def __init__(self, db_name="aas.db"):
        """
        :param db_name: path for the .db archive
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.tables_columns = self.get_tables_and_columns()
        self.tables_keys = self.get_primary_keys()
        # print(f"Conexão com o banco de dados {self.db_name} estabelecida")

    # ==================================================================================================================
    # ==================================================================================================================
    def _connection(self):
        """
        This function is not active
        :return: None
        """
        return

    def database(self):
        """
        This function is not activate
        :return: None
        """
        return

    # ==================================================================================================================
    # ==================================================================================================================
    def create_index(self, index_columns, order=None, unique=True):
        table_name = "students"
        if order is not None:
            print("Order is not used in SQLite")
        idx = list(index_columns)
        cursor = self.conn.cursor()
        if unique:
            unique_constraint = "UNIQUE"
        else:
            unique_constraint = ""
        index_name = f"{table_name}_idx"
        query = f"""
            CREATE {unique_constraint} INDEX IF NOT EXISTS {index_name} ON {table_name} ({', '.join(idx)});
        """
        cursor.execute(query)
        self.conn.commit()

    def create_index_by_collection(self, table_name, index_columns, order, unique=True):
        if order is not None:
            print("Order is not used in SQLite")
        idx = list(index_columns)
        cursor = self.conn.cursor()
        if unique:
            unique_constraint = "UNIQUE"
        else:
            unique_constraint = ""
        index_name = f"{table_name}_idx"
        query = f"""
            CREATE {unique_constraint} INDEX IF NOT EXISTS {index_name} ON {table_name} ({', '.join(idx)});
        """
        cursor.execute(query)
        self.conn.commit()

    # ==================================================================================================================
    async def insert_one(self, data):
        table_name = 'variable_history'
        if 'timestamp' in data.keys():
            data['timestamp'] = self.convert_timestamp(data['timestamp'])
        data = self.convert_data_to_string(data)
        self.insert_dict(table_name, data)

    async def insert_one_by_collection(self, table_name, data):
        if 'timestamp' in data.keys():
            data['timestamp'] = self.convert_timestamp(data['timestamp'])
        data = self.convert_data_to_string(data)
        self.insert_dict(table_name, data)

    # ==================================================================================================================
    # async \
    async def find(self, query, projection=None, size=1000):
        table_name = "variable_history"
        # await (self.find_by_collection(table_name,query,projection,size,None))
        return await self.find_by_collection(table_name, query, projection, size, None)

    # async \
    async def find_by_collection(self, table_name: str, query: dict, projection=None, size=1000, sort=None):

        columns = self.get_columns(table_name)

        query_sql, values = self.find_query_generator(table_name, columns, query, projection, sort, size)

        new_values = []

        for value in values:
            if isinstance(value, dict):
                if value.keys() == 'timestamp':
                    new_values.append(self.convert_timestamp(value))
            else:
                new_values.append(value)

        values = None

        self.cursor.execute(query_sql, new_values)
        results = self.cursor.fetchall()
        output = []

        for document in results:
            if projection is None:
                output.append(self.convert_data_to_dict(self.convert_data_to_zip(columns, document)))
            else:
                valid_keys = [key for key, value in projection.items() if value == 1]
                output.append(self.convert_data_to_dict(self.convert_data_to_zip(valid_keys, document)))

        return output

    # ==================================================================================================================
    # async\
    async def aggregation(self, query: dict, step: int, count: int = 0, size=1000):
        base_search = await self.find(query, size=size)
        filtered_search = [dado for dado in base_search if (dado["id"] % step) == 0]
        return filtered_search

    # ==================================================================================================================
    async def count(self):
        table_name = "variable_history"
        consulta_sql = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(consulta_sql)
        result = self.cursor.fetchone()[0]
        return result

    async def count_by_collection(self, table_name: str):
        query_sql = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(query_sql)
        result = self.cursor.fetchone()[0]
        return result

    # ==================================================================================================================
    """
    Auxiliary data manipulation and conversion functions
    """

    # ==================================================================================================================
    def find_query_generator(self, table_name: list, columns_names: list, query_conditions: dict, projection: dict,
                             sort, size):
        where = "WHERE" if len(query_conditions) > 0 else ""
        select_columns = "*"
        if projection:
            select_list = [coluna for coluna, valor in projection.items() if valor == 1]
            select_columns = ", ".join(select_list)
        query_sql = f"SELECT {select_columns} FROM {table_name} {where} "
        conditions = []
        values = []
        if '$and' in query_conditions:
            for condition in query_conditions['$and']:
                for key, subquery in condition.items():
                    if key == 'timestamp':
                        if '$gte' in subquery:
                            conditions.append('timestamp >= ?')
                            if isinstance(subquery['$gte'], dict):
                                timestamp_unix = self.convert_timestamp(subquery['$gte'])
                            else:
                                date_str = subquery['$gte'].replace('Z', '+00:00')
                                datetime_obj = datetime.fromisoformat(date_str)
                                timestamp_unix = int(datetime_obj.timestamp())
                            values.append(timestamp_unix)
                        if '$lte' in subquery:
                            conditions.append('timestamp <= ?')
                            if isinstance(subquery['$lte'], dict):
                                timestamp_unix = self.convert_timestamp(subquery['$lte'])
                            else:
                                date_str = subquery['$lte'].replace('Z', '+00:00')
                                datetime_obj = datetime.fromisoformat(date_str)
                                timestamp_unix = int(datetime_obj.timestamp())
                            values.append(timestamp_unix)
                    elif key == 'idShort':
                        conditions.append('idShort = ?')
                        values.append(subquery)

        for column in columns_names:
            if column in query_conditions:
                if isinstance(query_conditions[column], dict):
                    for op, value in query_conditions[column].items():
                        if op == "$gt":
                            conditions.append(f'{column} > ?')
                        elif op == "$gte":
                            conditions.append(f'{column} >= ?')
                        elif op == "$lt":
                            conditions.append(f'{column} < ?')
                        elif op == "$lte":
                            conditions.append(f'{column} <= ?')
                        values.append(value)
                else:
                    conditions.append(f'{column} = ?')
                    values.append(query_conditions[column])

        query_sql += " AND ".join(conditions)

        if sort is not None:
            for item in sort:
                if item[1] == 1:
                    query_sql += f" ORDER BY {item[0]} ASC"
                elif item[1] == -1:
                    query_sql += f" ORDER BY {item[0]} DESC"

        query_sql += f" LIMIT {size}"
        return query_sql, values

    def gerar_query(nome_tabela, ordem=None, limite=1000):
        conn = sqlite3.connect('seu_banco_de_dados.db')
        cursor = conn.cursor()

        # Substitua "seu_timestamp_column" pelo nome da coluna de timestamp.
        base_sql = f"SELECT * FROM {nome_tabela}"

        # Adicione a cláusula de ordenação se a ordem for especificada
        if ordem is not None:
            if ordem == 1:
                base_sql += " ORDER BY seu_timestamp_column ASC"
            elif ordem == -1:
                base_sql += " ORDER BY seu_timestamp_column DESC"

        # Adicione a cláusula LIMIT
        base_sql += f" LIMIT {limite}"

    def convert_data_to_zip(self, list, tuple):
        if len(list) != len(tuple):
            raise ValueError("The list and tuple must have the same number of elements.")
        result = dict(zip(list, tuple))
        return result

    def insert_dict(self, tabela, dicionario):
        """
        Inserts a dictionary into a table, but if the table does not exist, it creates the table and the fields before inserting the data.
        :param tabela: Table name
        :param dicionario: Dictionary
        :return:
        """
        self.cursor.execute(f"PRAGMA table_info({tabela})")
        if not self.cursor.fetchall():
            create_table_sql = f"CREATE TABLE {tabela} ({', '.join(f'{chave} {type(valor).__name__}' for chave, valor in dicionario.items())})"
            self.cursor.execute(create_table_sql)
        insert_sql = f"INSERT INTO {tabela} ({', '.join(dicionario.keys())}) VALUES ({', '.join(['?'] * len(dicionario))})"
        self.cursor.execute(insert_sql, list(dicionario.values()))
        self.conn.commit()

    def list_verifier(self, list1: list, list2: list):
        """
        Checks if the contents of list1 exist in list2
        :param list1:
        :param list2:
        :return: Boolean
        """
        set1 = set(list1)
        set2 = set(list2)
        return set1.issubset(set2)

    def convert_timestamp(self, obj):
        """
        Converts a timstamp object to a UNIX timestamp
        :param obj: timestamp (datetime)
        :return: UNIX timestamp
        """
        if '$date' in obj:
            date_str = obj['$date'].replace('Z', '+00:00')
            datetime_obj = datetime.fromisoformat(date_str)
            timestamp_unix = int(datetime_obj.timestamp())
            return timestamp_unix
        else:
            return obj

    def convert_data_to_string(self, d):
        """
        Converts dictionaries within another dictionary to a JSON string
        :param d: A dictionary with another dictionary inside
        :return: A dictionary with a JSON string
        """
        for chave, valor in d.items():
            if isinstance(valor, dict):
                d[chave] = json.dumps(valor)
            elif isinstance(valor, list):
                for i, item in enumerate(valor):
                    if isinstance(item, dict):
                        valor[i] = json.dumps(item)
            elif isinstance(valor, tuple):
                valor_lista = list(valor)
                for i, item in enumerate(valor_lista):
                    if isinstance(item, dict):
                        valor_lista[i] = json.dumps(item)
                d[chave] = tuple(valor_lista)
        return d

    def convert_data_to_dict(self, d):
        """
        Converts stored values in JSON string format inside a dictionary to a dictionary
        :param d: Input dictionary with JSON strings in values
        :return: Dictionary converted
        """
        for chave, valor in d.items():
            if isinstance(valor, str):
                try:
                    valor_parseado = json.loads(valor)
                    if isinstance(valor_parseado, dict):
                        d[chave] = self.convert_data_to_dict(valor_parseado)
                    elif isinstance(valor_parseado, list):
                        for i, item in enumerate(valor_parseado):
                            if isinstance(item, dict):
                                valor_parseado[i] = self.convert_data_to_dict(item)
                        d[chave] = valor_parseado
                except json.JSONDecodeError:
                    pass
        return d

    # ==================================================================================================================
    """
    Sqlite manipulation auxiliary functions
    """
    # ==================================================================================================================
    def create_table(self, table_name, columns):
        """
        :param table_name: Table name
        :param columns: Create the columns. Ex: "name TEXT, age INTEGER"
        :return: None
        """
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.tables_columns = self.get_tables_and_columns()
        self.tables_keys = self.get_primary_keys()

    def drop_table(self, table_name):
        """
        Deletes tables from the database
        :param table_name: Table name
        :return: None
        """
        cursor = self.conn.cursor()
        query = f"DROP TABLE IF EXISTS {table_name};"
        cursor.execute(query)
        self.conn.commit()

    def get_tables_and_columns(self):
        """
        SQL query to get the names of all tables and columns in the database
        :return: Dictionary containing tables(key) and their columns(value)
        """
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(tables_query)
        tables = self.cursor.fetchall()
        table_info = {}
        for table in tables:
            table_name = table[0]
            columns_query = f"PRAGMA table_info({table_name});"
            self.cursor.execute(columns_query)
            columns_info = self.cursor.fetchall()
            column_names = [column[1] for column in columns_info]
            table_info[table_name] = column_names
        return table_info

    def get_tables(self):
        """
        SQL query to get the list of tables in the database
        :return: List with table names
        """
        query = f"SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(query)
        tables = self.cursor.fetchall()
        table_names = [table[0] for table in tables]
        return table_names

    def get_columns(self, table_name):
        """
        SQL query to get the list of columns names in a table in the database
        :param table_name: Table name
        :return: List with columns names
        """
        query = f"PRAGMA table_info({table_name});"
        self.cursor.execute(query)
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names

    def get_primary_keys(self):
        """
        SQL query to get the names of all tables in the database
        :return: Dict with tables(key) and primary keys(value)
        """
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(tables_query)
        tables = self.cursor.fetchall()
        table_keys = {}
        for table in tables:
            table_name = table[0]
            columns_query = f"PRAGMA table_info({table_name});"
            self.cursor.execute(columns_query)
            columns_info = self.cursor.fetchall()
            primary_key = None
            for column in columns_info:
                name = column[1]
                pk = column[5]
                if pk == 1:
                    primary_key = name
                    break
            if primary_key:
                table_keys[table_name] = primary_key
        return table_keys

    def fetch_data(self, table_name, condition=None):
        """
        Basic method to obtain data from a table
        :param table_name: table name
        :param condition: condition after WHERE, e.g. " name = "Arthur" "
        :return: table data
        """
        condition = f"WHERE {condition}" if condition else ""
        select_data_sql = f"SELECT * FROM {table_name} {condition}"
        self.cursor.execute(select_data_sql)
        return self.cursor.fetchall()

    async def close(self):
        """
        Closes the connection to the database
        :return: None
        """
        if self.conn:
            self.conn.close()

    def __del__(self):
        if self.conn:
            self.conn.close()
        # print(f"Conexão com o banco de dados {self.db_name} fechada")
