import sqlite3
import json
from datetime import datetime


class SQLiteDB:
    """
    SQLITE by ONPPROD
    """
    def __init__(self, db_name):
        """
        :param db_name: path for the .db archive
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.tables_columns = self.get_tables_and_columns()
        self.tables_keys = self.get_primary_keys()
        print(f"Conexão com o banco de dados {self.db_name} estabelecida")

    # ==================================================================================================================
    # ==================================================================================================================
    def _connection(self):
        """
        This function is not active
        :return: None
        """
        pass

    def database(self):
        """
        This function is not activate
        :return: None
        """
        pass

    def create_index(self, index_columns, order, unique=True):
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

    def insert_one(self, data):
        print(data)
        table_name = 'variable_history'
        timest = data['timestamp']
        print(type(timest))
        data['timestamp'] = self.convert_timestamp(timest)
        data = self.convert_data_to_string(data)
        cursor = self.conn.cursor()

        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())

        query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders});
        """

        tables = self.get_tables()
        if table_name not in tables:
            create_query = ''
            for column in data.keys():
                create_query += column + ","
            self.create_table(table_name, create_query[:-1])

        cursor.execute(query, values)
        self.conn.commit()

    async def insert_one_by_collection(self, table_name, data):
        data['timestamp'] = self.convert_timestamp(data['timestamp'])
        data = self.convert_data_to_string(data)
        cursor = self.conn.cursor()
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())
        query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders});
        """
        await cursor.execute(query, values)
        await self.conn.commit()

    async def find(self, query, projection=None, size=1000):
        cursor = self.conn.cursor()
        # Construa a consulta SQL com base nos critérios de pesquisa
        query_sql = "SELECT * FROM variable_history WHERE "
        conditions = []
        values = []

        if '$and' in query:
            for condition in query['$and']:
                for key, subquery in condition.items():
                    if key == 'timestamp':
                        if '$gte' in subquery:
                            conditions.append('timestamp >= ?')
                            values.append(subquery['$gte'])
                        if '$lte' in subquery:
                            conditions.append('timestamp <= ?')
                            values.append(subquery['$lte'])
                    elif key == 'idShort':
                        conditions.append('idShort = ?')
                        values.append(subquery)

        query_sql += " AND ".join(conditions)
        query_sql += f" LIMIT {size}"
        # Execute a consulta SQL
        cursor.execute(query_sql, values)
        # Recupere os resultados
        results = cursor.fetchall()

        if projection:
            # Verifica a projeção e ajusta a consulta SQL
            if 'value' in projection and projection['value'] == 1:
                print()
                #query_sql = query_sql.replace("*", "value")
            if '_id' in projection and projection['_id'] == 0:
                print()
                #query_sql = query_sql.replace(", _id", "")

        return results



    # ==================================================================================================================
    def fetch_data(self, table_name, condition=None):
        """
        :param table_name: table name in the database
        :param condition: condition after WHERE for  in the database
        :return: table with data
        """
        condition = f"WHERE {condition}" if condition else ""
        select_data_sql = f"SELECT * FROM {table_name} {condition}"
        self.cursor.execute(select_data_sql)
        return self.cursor.fetchall()

    """
    Auxiliary functions for the operation of SQLITE
    """
    # ==================================================================================================================
    def create_table(self, table_name, columns):
        """
        :param table_name: insert the table name
        :param columns: create the columns. Ex: "name TEXT, age INTEGER"
        :return:
        """
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.tables_columns = self.get_tables_and_columns()
        self.tables_keys = self.get_primary_keys()

    def drop_table(self, table_name):
        cursor = self.conn.cursor()
        query = f"DROP TABLE IF EXISTS {table_name};"
        cursor.execute(query)
        self.conn.commit()

    def get_tables_and_columns(self):
        # Consulta SQL para obter os nomes de todas as tabelas no banco de dados
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
        Consulta SQL para obter a lista de tabelas no banco de dados
        :return: List with table values
        """
        query = f"SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(query)
        tables = self.cursor.fetchall()
        table_names = [table[0] for table in tables]
        return table_names

    def get_columns(self, table_name):
        """
        Consulta SQL para obter a lista de colunas em uma tabela no banco de dados
        :param table_name:
        :return: List with columns values
        """
        query = f"PRAGMA table_info({table_name});"
        self.cursor.execute(query)
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names

    def get_primary_keys(self):
        """
        Consulta SQL para obter os nomes de todas as tabelas no banco de dados
        :return:
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

    def list_verifier(self, lista1: list, lista2: list):
        """
        Verifica se o conteudo da lista1 existe na lista2
        :param lista1:
        :param lista2:
        :return:
        """
        conjunto1 = set(lista1)
        conjunto2 = set(lista2)
        return conjunto1.issubset(conjunto2)

    def convert_timestamp(self,obj):
        if '$date' in obj:
            date_str = obj['$date'].replace('Z', '+00:00')
            datetime_obj = datetime.fromisoformat(date_str)
            timestamp_unix = int(datetime_obj.timestamp())
            return timestamp_unix
        else:
            return obj

    def convert_data_to_string(self, d):
        for chave, valor in d.items():
            if isinstance(valor, dict):
                # Se o valor é um dicionário, converte para JSON
                d[chave] = json.dumps(valor)
            elif isinstance(valor, list):
                # Se o valor é uma lista, verificamos se há dicionários nas listas e os convertemos
                for i, item in enumerate(valor):
                    if isinstance(item, dict):
                        valor[i] = json.dumps(item)
            elif isinstance(valor, tuple):
                # Se o valor é uma tupla, verificamos se há dicionários nas tuplas e os convertemos
                valor_lista = list(valor)
                for i, item in enumerate(valor_lista):
                    if isinstance(item, dict):
                        valor_lista[i] = json.dumps(item)
                d[chave] = tuple(valor_lista)

        return d

    def convert_data_to_dict(self, d):
        for chave, valor in d.items():
            if isinstance(valor, str):
                try:
                    # Tenta analisar o valor como JSON
                    valor_parseado = json.loads(valor)
                    if isinstance(valor_parseado, dict):
                        # Se o valor é um dicionário, substitui o valor original
                        d[chave] = self.convert_data_to_dict(valor_parseado)
                    elif isinstance(valor_parseado, list):
                        # Se o valor é uma lista, verifica se há dicionários nas listas
                        for i, item in enumerate(valor_parseado):
                            if isinstance(item, dict):
                                valor_parseado[i] = self.convert_data_to_dict(item)
                        d[chave] = valor_parseado
                except json.JSONDecodeError:
                    # Se não for um JSON válido, mantém o valor original
                    pass
        return d

    def get_tables(self):
        """
        :return: List with table values
        """
        # Consulta SQL para obter a lista de tabelas no banco de dados
        query = f"SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(query)
        # Recupera os resultados da consulta
        tables = self.cursor.fetchall()
        # Extrai os nomes das tabelas a partir dos resultados
        table_names = [table[0] for table in tables]

        return table_names

    def close(self):
        self.conn.close()

    def __del__(self):
        self.close()
        print(f"Conexão com o banco de dados {self.db_name} fechada")
