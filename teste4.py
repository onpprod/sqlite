import pymongo

# Conecte-se ao seu servidor MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")

# Acesse o banco de dados e a coleção desejados
db = client["seu_banco_de_dados"]
collection = db["sua_colecao"]

# Defina os campos de projeção
projection = {"nome": 1, "idade": 0}

# Faça a consulta com a projeção
result = collection.find_by_collection({}, projection)

# Itere sobre os resultados e imprima-os
for doc in result:
    print(doc)

# Feche a conexão com o servidor MongoDB
client.close()
