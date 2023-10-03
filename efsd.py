import sqlite3

def insert_one(tabela, dicionario, db_file):
    # Conecte-se ao banco de dados SQLite
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Verifique se a tabela existe e, se não existir, crie-a
    cursor.execute(f"PRAGMA table_info({tabela})")
    if not cursor.fetchall():
        # Crie a tabela com colunas baseadas nas chaves e tipos do dicionário
        create_table_sql = f"CREATE TABLE {tabela} ({', '.join(f'{chave} {type(valor).__name__}' for chave, valor in dicionario.items())})"
        cursor.execute(create_table_sql)

    # Insira os dados do dicionário na tabela
    insert_sql = f"INSERT INTO {tabela} ({', '.join(dicionario.keys())}) VALUES ({', '.join(['?'] * len(dicionario))})"
    cursor.execute(insert_sql, list(dicionario.values()))

    # Comita a transação e feche a conexão
    conn.commit()
    conn.close()

# Exemplo de uso
if __name__ == '__main__':
    dicionario_exemplo = {
        'nome': 'Alex',
        'idade': 30,
        'cidade': 'São Paulo'
    }
    db_file = 'seubanco.db'
    tabela = 'caral'
    insert_one(tabela, dicionario_exemplo, db_file)
