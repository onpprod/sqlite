# Dicionário com valores MongoDB
data = {
    '_id': {'$oid': '64c3ee842b13e057bf0d8e22'},
    'idShort': 'ReactivePower',
    'category': 'VARIABLE',
    'value': {
        'value': 0,
        'data_type': 6,
        'source_timestamp': {'$date': '2023-07-28T16:36:17.935Z'},
        'server_timestamp': None
    },
    'valueType': 11,
    'timestamp': {'$date': '2023-07-28T16:36:20.158Z'},
    'id': 31
}

# Dicionário com a data e hora no formato original
# data = {
#     'timestamp': {'$date': '2023-07-28T16:36:20.158Z'}
# }

from datetime import datetime
import json


def convert_data_in(d):
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

# Função para converter a data e hora em um formato aceito pelo SQLite
def convert_to_sqlite_timestamp(obj):
    if '$date' in obj:
        date_str = obj['$date'].replace('Z', '+00:00')
        datetime_obj = datetime.fromisoformat(date_str)
        timestamp_unix = int(datetime_obj.timestamp())
        return timestamp_unix
    else:
        return obj


# Converte a data e hora no dicionário
data['timestamp'] = convert_to_sqlite_timestamp(data['timestamp'])

data = convert_data_in(data)
print(type(data["value"]))
print(type(data['timestamp']))

# Imprime o dicionário com a data e hora convertida
print(data)
