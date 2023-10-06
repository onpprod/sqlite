from tinydb import TinyDB, Query

class TinyDBManager:
    def __init__(self, db_path):
        self.db = TinyDB(db_path)

    def _connection(self):
        pass

    def database(self):
        pass

    def create_index(self, index, order, unique=True):
        # Obtenha uma lista de todas as tabelas no banco de dados
        colecoes = self.db.tables()
        # Itere sobre as tabelas e crie índices para cada uma
        for colecao in colecoes:
            table = self.db.table(colecao)
            # Verifique se a coluna indexada existe na tabela
            if index in table.all()[0]:
                # Mapeia a ordem do índice MongoDB para o TinyDB
                order_mapping = {
                    'asc': 1,
                    'desc': -1
                }
                order_tinydb = order_mapping.get(order, 1)

                index_spec = (index, order_tinydb, unique)
                table.create_index(index_spec)

    def create_index_by_collection(self, collection, index, order, unique=True):
        # Verifique se a coluna indexada existe na tabela
        if index in self.db.table(collection).all()[0]:
            # Mapeia a ordem do índice MongoDB para o TinyDB
            order_mapping = {
                'asc': 1,
                'desc': -1
            }
            order_tinydb = order_mapping.get(order, 1)

            index_spec = (index, order_tinydb, unique)
            self.db.table(collection).create_index(index_spec)

    def insert_one(self, document):
        # Insere um documento na tabela "variable_history"
        table = self.db.table('variable_history')
        table.insert(document)

    def insert_one_by_collection(self, collection, document):
        # Inserir um documento em uma coleção específica passada como parâmetro
        table = self.db.table(collection)
        table.insert(document)

    def find(self, query, projection=None, size=1000):
        # Acesse a tabela "variable_history"
        table = self.db.table('variable_history')

        # Crie um objeto Query
        q = Query()

        # Se uma projeção for fornecida, use-a
        if projection:
            result = table.search((q.matches(query)) & (q.search(query)))
            result = [{k: v for k, v in doc.items() if k in projection} for doc in result]
        else:
            result = table.search(q.matches(query))

        return result[:size]

    def find_by_collection(self, collection, query, projection=None, size=1000, sort=None):
        if sort is None:
            sort = []
        # Acesse a coleção (tabela) especificada
        table = self.db.table(collection)
        # Crie um objeto Query
        q = Query()
        # Se uma projeção for fornecida, use-a
        if projection:
            result = table.search((q.matches(query)) & (q.search(query)))
            result = [{k: v for k, v in doc.items() if k in projection} for doc in result]
        else:
            result = table.search(q.matches(query))
        if sort:
            for campo, ordem in sort:
                if ordem == -1:
                    result = sorted(result, key=lambda doc: doc.get(campo), reverse=True)
                else:
                    result = sorted(result, key=lambda doc: doc.get(campo))
        return result[:size]

    def aggregation(self, collection, query, step, count, size=1000):
        table = self.db.table(collection)
        q = Query()

        result = [doc for doc in table.search(q.matches(query)) if doc['id'] % step == 0]

        return result[:size]

    def count(self, collection, query):
        table = self.db.table(collection)
        q = Query()
        return len(table.search(q.matches(query)))

def main():
    # Exemplo de uso:

    # Criar uma instância do TinyDBManager
    db_manager = TinyDBManager('seu_banco_de_dados.json')

    # Inserir um documento em uma coleção
    db_manager.insert_one('variable_history', {"id": 1, "nome": "Alice"})

    # Realizar uma consulta semelhante à do MongoDB
    resultado = db_manager.find_by_collection('variable_history', {"nome": "Alice"})
    print(resultado)

    # Realizar uma operação de agregação
    resultado_agregado = db_manager.aggregation('variable_history', {"nome": "Alice"}, 3, 10)
    print(resultado_agregado)

    # Contar documentos na coleção
    contagem = db_manager.count('variable_history', {"nome": "Alice"})
    print(contagem)

if __name__ == "__main__":
    main()