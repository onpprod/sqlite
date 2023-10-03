import sqlite3


class SQLiteDB:
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

    def insert_data(self, table_name, data_to_insert, ignore_primary_key=True):
        # gerar os campos
        fields = ''
        names = self.tables_columns
        if ignore_primary_key:
            for field in self.tables_columns[table_name][1:]:
                fields += field + ','
        else:
            for field in self.tables_columns[table_name]:
                fields += field + ','
        # gerar os marcadores
        placeholders = ', '.join(['?'] * len(data_to_insert))
        insert_data_sql = f"INSERT INTO {table_name} ({fields[:-1]}) VALUES ({placeholders})"
        self.cursor.execute(insert_data_sql, data_to_insert)
        self.conn.commit()

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

    def update_data(self, table_name, data, condition=None):
        condition = f"WHERE {condition}" if condition else ""
        update_data_sql = f"UPDATE {table_name} SET {data} {condition}"
        self.cursor.execute(update_data_sql)
        self.conn.commit()

    def delete_data(self, table_name, condition=None):
        condition = f"WHERE {condition}" if condition else ""
        delete_data_sql = f"DELETE FROM {table_name} {condition}"
        self.cursor.execute(delete_data_sql)
        self.conn.commit()

    def query_generator(self, table_name, *args, **kwargs):
        # check keys in kwargs
        kwargs_keys = list(kwargs.keys())
        print("kwargs: ",kwargs_keys)
        columns = list(self.tables_columns[table_name])
        print("tables: ", columns)
        # check if the columns exists
        if kwargs_keys is not None:
            if not self.list_verifier(kwargs_keys, columns):
                print("Pesquisa especifica nao disponivel para as chaves inseridas.")
                return
        # query base
        query_args = ''
        query_kwargs = ''
        # query for kwargs
        if kwargs_keys is not None:
            for key in kwargs_keys:
                query_kwargs += f' {key}={kwargs[key]},'
        # query for args
        if args is not None:
            for key in args:
                query_args += f' {key}=?,'
        # query complete
        query = f"SELECT * FROM {table_name} WHERE{query_kwargs[:-1]}{query_args[:-1]}"
        return query

    def safe_insert_data(self, table_name, data):
        existing_data = self.fetch_data(table_name, f"name = ? AND age = ?")
        if existing_data:
            print(f"O elemento com nome '{data[0]}' e idade {data[1]} já existe. Não foi inserido.")
        else:
            self.insert_data(table_name, data)
            print(f"Elemento com nome '{data[0]}' e idade {data[1]} inserido com sucesso.")

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

    def get_columns(self, table_name):
        """
        :param table_name:
        :return: List with columns values
        """
        # Consulta SQL para obter os nomes das colunas de uma tabela
        query = f"PRAGMA table_info({table_name});"
        self.cursor.execute(query)

        # Recupera os resultados da consulta
        columns_info = self.cursor.fetchall()

        # Extrai os nomes das colunas a partir dos resultados
        column_names = [column[1] for column in columns_info]

        return column_names

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

    def get_primary_keys(self):
        # Consulta SQL para obter os nomes de todas as tabelas no banco de dados
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

    def check_data_exists(self, table_name, value, ignore_primary_key=True):
        # Consulta SQL para verificar se um dado específico existe na tabela
        # gerar os campos
        query_base = ''
        columns = list(self.tables_columns[table_name])
        keys = self.tables_keys[table_name]
        print("colunas:", columns)
        print("keys:", keys)
        if ignore_primary_key:
            columns.remove(keys)
            for field in columns:
                query_base += field + ' = ? AND '
        else:
            for field in self.tables_columns[table_name]:
                query_base += field + '=?,'

        query = f"SELECT COUNT(*) FROM {table_name} WHERE {query_base[:-4]}"
        print(query)
        self.cursor.execute(query, value)
        result = self.cursor.fetchone()
        return result[0] > 0

    def close(self):
        self.conn.close()

    def __del__(self):
        self.close()
        print(f"Conexão com o banco de dados {self.db_name} fechada")
