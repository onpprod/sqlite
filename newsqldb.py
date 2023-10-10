import aiosqlite
import json
from datetime import datetime
import asyncio

class SQLiteDB:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_name)
        self.conn.row_factory = aiosqlite.Row

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def create_table(self, table_name, columns):
        async with self.conn.cursor() as cursor:
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            await cursor.execute(create_table_sql)

    async def insert_one_by_collection(self, table_name, data):
        data['timestamp'] = self.convert_timestamp(data['timestamp'])
        data = self.convert_data_to_string(data)
        await self.insert_dict(table_name, data)

    async def find(self, query, projection=None, size=1000):
        table_name = "variable_history"
        async with self.conn.cursor() as cursor:
            return await self.find_by_collection(table_name, cursor, query, projection, size, None)

    async def find_by_collection(self, table_name, cursor, query, projection=None, size=1000, sort=None):
        columns = await self.get_columns(table_name, cursor)
        query_sql, values = self.find_query_generator(table_name, columns, query, projection, sort, size)
        new_values = []

        for value in values:
            if isinstance(value, dict):
                if 'timestamp' in value:
                    new_values.append(self.convert_timestamp(value))
            else:
                new_values.append(value)

        await cursor.execute(query_sql, new_values)
        results = await cursor.fetchall()
        output = []

        for document in results:
            if projection is None:
                output.append(self.convert_data_to_dict(self.convert_data_to_zip(columns, document)))
            else:
                valid_keys = [key for key, value in projection.items() if value == 1]
                output.append(self.convert_data_to_dict(self.convert_data_to_zip(valid_keys, document)))

        return output

    async def aggregation(self, query: dict, step: int, count: int = 0, size=1000):
        pesquisa_base = await self.find(query, size=size)
        pesquisa_filtrada = [dado for dado in pesquisa_base if (dado["id"] / step) == 0]
        return pesquisa_filtrada

    async def get_columns(self, table_name, cursor):
        query = f"PRAGMA table_info({table_name});"
        await cursor.execute(query)
        columns_info = await cursor.fetchall()
        column_names = [column['name'] for column in columns_info]
        return column_names

    # Outros métodos da sua classe

    async def convert_timestamp(self, obj):
        if '$date' in obj:
            date_str = obj['$date'].replace('Z', '+00:00')
            datetime_obj = datetime.fromisoformat(date_str)
            timestamp_unix = int(datetime_obj.timestamp())
            return timestamp_unix
        else:
            return obj

    async def convert_data_to_string(self, d):
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

    async def convert_data_to_dict(self, d):
        for chave, valor in d.items():
            if isinstance(valor, str):
                try:
                    valor_parseado = json.loads(valor)
                    if isinstance(valor_parseado, dict):
                        d[chave] = await self.convert_data_to_dict(valor_parseado)
                    elif isinstance(valor_parseado, list):
                        for i, item in valor_parseado:
                            if isinstance(item, dict):
                                valor_parseado[i] = await self.convert_data_to_dict(item)
                        d[chave] = valor_parseado
                except json.JSONDecodeError:
                    pass
        return d

    async def convert_data_to_zip(self, lista, tupla):
        if len(lista) != len(tupla):
            raise ValueError("A lista e a tupla devem ter o mesmo número de elementos.")
        resultado = dict(zip(lista, tupla))
        return resultado

    async def find_query_generator(self, table_name, columns_names, query_conditions, projection, sort, size):
        # Código para gerar a consulta SQL
        pass

    async def insert_dict(self, tabela, dicionario):
        # Código para inserir um dicionário na tabela
        pass
