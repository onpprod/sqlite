from motor.motor_asyncio import AsyncIOMotorClient
from core.environments import env
# from core.constants import EventLog


class Mongo:
    def __init__(self):
        self.host = env('DATABASE_HOST')
        self.port = int(env('DATABASE_PORT'))
        self.username = env('DATABASE_USER')
        self.password = env('DATABASE_PASS')
        self.connection = self._connection()

    def _connection(self):
        return AsyncIOMotorClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password
        )

    def database(self):
        return self.connection.asset_administration_shell

    def create_index(self, index, order, unique=True):
        db = self.database()
        db.variable_history.create_index([(index, order)], unique=unique)

    def create_index_by_collection(self, collection, index, order, unique=True):
        db = self.database()
        db[collection].create_index([(index, order)], unique=unique)

    async def insert_one(self, document: dict):
        db = self.database()
        await db.variable_history.insert_one(dict(document))

        return db.variable_history

    async def insert_one_by_collection(self, collection, document):
        db = self.database()
        await db[collection].insert_one(document)

    async def find(self, query: dict, projection=None, size=1000):
        db = self.database()

        cursor = db.variable_history.find(query, projection).limit(size)
        documents = await cursor.to_list(size)
        return documents

    async def find_by_collection(self, collection: str, query: dict, projection=None, size=1000, sort=None):
        if sort is None:
            sort = []

        if projection is None:
            projection = {}

        db = self.database()
        cursor = db[collection].find(query, projection).limit(size).sort(sort)

        return await cursor.to_list(size)

    async def find_by_collection_to_cursor(self, collection: str, query: dict, sort=None, size=1000):
        if sort is None:
            sort = []

        db = self.database()
        return db[collection].find(query).limit(size).sort(sort)

    async def aggregation(self, query: dict, step: int, count: int, size=1000):
        db = self.database()
        pipeline = [
            {'$match': query},
            {
                '$redact': {
                    '$cond': {
                        'if': {
                            '$eq':[
                               {
                            '$mod': [
                                '$id', step
                            ]
                        }, 0
                            ]
                        },
                        'then': '$$KEEP',
                        'else': '$$PRUNE'
                    }
                }
            }
        ]

        cursor = db.variable_history.aggregate(pipeline)
        documents = await cursor.to_list(length=None)
        return documents

    async def count(self, query) -> int:
        db = self.database()
        return await db.variable_history.count_documents(query)

    async def count_by_collection(self, collection: str, query: dict) -> int:
        db = self.database()

        return await db[collection].count_documents(query)