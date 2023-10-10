from sqldatabase import SQLiteDB

projection = {"nome": 1, "idade": 1}

# out_data = db.find(query, projection, size=10)

db = SQLiteDB("projection.db")
start = {"$date":"2022-07-01T16:36:20.158Z"}
end = {"$date":"2024-07-28T16:36:20.158Z"}

query = {
                '$and': [
                    {'timestamp': {'$gte': start}},
                    {'timestamp': {'$lte': end}},
                    {'idShort': str("ReactivePower")}
                ]
            }

projection = {'_id': 0, 'value': 1}

sort = [('timestamp', -1)]

query_ = db.find_query_generator("teste_table",
                                ['a','b','c'],
                                query,
                                projection,
                                None,
                                10)

print(query_)




