import time

from sqldatabase import SQLiteDB

db = SQLiteDB("teste.db")
inicio = time.time()
# =====================================

data1 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e22"},"idShort":"ReactivePower","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.935Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.158Z"},"id":31}
data2 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e0a"},"idShort":"XVibrationAccelerationDeviation","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.738Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.096Z"},"id":7}
data3 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e11"},"idShort":"YVibrationAccelerationSkewness","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.797Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.107Z"},"id":14}
data4 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e1c"},"idShort":"Current","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.885Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.135Z"},"id":25}
data5 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e12"},"idShort":"YVibrationAccelerationDeviation","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.805Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.108Z"},"id":15}
data6 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e20"},"idShort":"Frequency","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.92Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.155Z"},"id":29}
data7 = {"_id":{"$oid":"64c3ee842b13e057bf0d8e23"},"idShort":"ApparentPower","category":"VARIABLE","value":{"value":0,"data_type":6,"source_timestamp":{"$date":"2023-07-28T16:36:17.943Z"},"server_timestamp":None},"valueType":11,"timestamp":{"$date":"2023-07-28T16:36:20.161Z"},"id":32}
data = [data1,data2,data3,data4,data5,data6,data7]
start = {"$date":"2023-07-27T16:36:20.158Z"}
end = {"$date":"2023-07-29T16:36:20.158Z"}

query = {
                '$and': [
                    {'timestamp': {'$gte': start}},
                    {'timestamp': {'$lte': end}},
                    {'idShort': str("ReactivePower")}
                ]
            }

for doc in data:
    db.insert_one(doc)

tac = db.get_tables()
print(tac)

print(db.count())
print(db.fetch_data("variable_history"))

print("Dados pesquisados: ")
print(db.find({}, size=1))

db.drop_table("variable_history")

# =====================================
fim = time.time()
print(fim-inicio)
db.close()