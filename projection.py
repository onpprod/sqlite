from sqldatabase import SQLiteDB
projection = {"nome": 1, "idade": 1}

db = SQLiteDB("projection.db")
start = {"$date":"2022-07-01T16:36:20.158Z"}
end = {"$date":"2024-07-28T16:36:20.158Z"}
query_mongo = {
    '$and': [
        {'timestamp': {'$gte': start}},
        {'timestamp': {'$lte': end}},
        {'idShort': str("ReactivePower")}
    ]
}

query = db.find_query_generator("teste_table",
                                ['a','b','c'],
                                query_mongo,
                                {"a":1},
                                [('timestamp', -1)],
                                10)

print(query)

order_mapping = {
                'asc': 1,
                'desc': -1
            }
order = [('timestamp', -1)]
# order_tinydb = order_mapping.get(order, 1)



