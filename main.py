from sqlitedb import SQLiteDB
import time

# Exemplo de uso da classe SQLiteDB
if __name__ == "__main__":

    db = SQLiteDB("my_database.db")
    db.create_table("students", "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER")
    data = db.fetch_data("students")
    print("Dados na tabela 'students':", data[0])
    print("Quantidade: ", len(data))

    tabelas = db.tables_columns
    chaves = db.tables_keys
    print(tabelas)
    print(chaves)

    begin = time.time()
    n_insert = 1
    for i in range(n_insert):
        db.insert_data("students", ("Alice", 23))
    end = time.time()

    total = end-begin
    print("Tempo de :", total, " s")
    print("Tempo por insercao: ",n_insert/total," i/s")

    nome_arquivo = "teste_sqlite.txt"

    with open(nome_arquivo, 'a') as arquivo:
        arquivo.write(str(total) + '\n')

    # # Inserção de dados sem especificar o valor da coluna "id"
    # db.insert_data("students", ("Alice", 22))
    # print(db.check_data_exists("students",("Alice", 22)))
    # db.insert_data("students", ("Bob", 24))
    # data = db.fetch_data("students")
    # print("Dados na tabela 'students' após a inserção:", data)
    #
    # # Utilizando a função safe_insert_data
    # db.safe_insert_data("students", ("Alice", 22))  # Não deve ser inserido, pois já existe
    # db.safe_insert_data("students", ("Charlie", 22))  # Deve ser inserido, pois é único
    #
    # # Utilizando as funções de pesquisa personalizadas
    # search_result = db.search_by_name("students", "Alice")
    # print("Resultado da pesquisa por nome:", search_result)
    #
    # search_result = db.search_by_age_range("students", 20, 25)
    # print("Resultado da pesquisa por faixa etária:", search_result)

    db.close()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
