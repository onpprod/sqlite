from sqldatabase import SQLiteDB

db = SQLiteDB("teste.db")
data = {"nome":"Otavio", "idade": 15, "curso": {'Alfabetizacao': 'Gurilandia', 'Ensino medio': 'Carmo'}, 'timestamp': {'$date': '2023-07-28T16:36:20.158Z'}}

# ======================================================================================================================
print("Teste de criacao de tabela")
db.create_table("teste", "nome TEXT , idade INTEGER, curso TEXT, timestamp TIMESTAMP")
print(db.get_tables_and_columns())
print()
# ======================================================================================================================
print("Teste de destruicao de tabela")
db.drop_table("teste")
print(db.get_tables_and_columns())
print()
# ======================================================================================================================
print("Teste de conversao de dados 1 - string")
data_s = db.convert_data_to_string(data)
print(data_s)
print(type(data_s['curso']))
print()
# ======================================================================================================================
print("Teste de conversao de dados 2 - dict")
data_back = db.convert_data_to_dict(data_s)
print(data_back)
print(type(data_back['curso']))
print()
# ======================================================================================================================
print("Teste")
data['timestamp'] = db.convert_timestamp(data['timestamp'])
print(data)
print()
# ======================================================================================================================
print("Teste")
data = {"nome":"Otavio", "idade": 15, "curso": {'Alfabetizacao': 'Gurilandia', 'Ensino medio': 'Carmo'}, 'timestamp': {'$date': '2023-07-28T16:36:20.158Z'}}
data = db.convert_data_to_string(data)
db.insert_one(data)
d = db.fetch_data("tabela")
print(d)







# db = SQLiteDB("teste_1.db")
# # db.drop_table("students")
# # db.create_table("students", "nome TEXT, age INTEGER")
# # db.create_index_by_collection("students", ["age"], 1)
# # print("DB: ", db.get_tables_and_columns())
#
# print()
#
# db.close()

# test1 = db.query_generator("students", "cavalo", "vaca")
# print(f"Teste 1 : {test1}\n")
# print("========================================================================")
# test2 = db.query_generator("students", nome="otavio")
# print(f"Teste 2 : {test2}\n")
