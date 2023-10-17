import asyncio
import time

from sqldatabase import SQLiteDB

def top_print(texto_central, largura_total=20, caractere_preenchimento="="):
    texto_formatado = texto_central.center(largura_total, caractere_preenchimento)
    print(texto_formatado)

db = SQLiteDB()
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

projection = {'_id': 0, 'value': 1}
db.drop_table("variable_history")

async def main():
    # ==================================================================================================================
    top_print("Teste [insert_one]",100)
    print("Tabelas Antes: ", db.get_tables())
    for doc in data:
        await db.insert_one(doc)
    print("Tabelas Depois: ", db.get_tables())

    # ==================================================================================================================
    top_print("Teste [count]",100)
    print(f"Count: {await db.count()}")

    # ==================================================================================================================
    top_print("Teste [find] without query",100)
    out_data = await db.find({},size=1)
    print(f"Data: {out_data}")

    # ==================================================================================================================
    top_print("Teste [find] with query",100)
    out_data = await db.find(query, size=1)
    print(f"Data: {out_data}")

    # ==================================================================================================================
    top_print("Teste [find] with query & projection",100)
    """
    O find com projection retorna somente as colunas 
    """
    out_data = await db.find(query, projection, size=1)
    print(f"Data: {out_data}")

    # ==================================================================================================================
    top_print("Teste [aggregation]",100)

    out_data = await db.aggregation(query,1)
    print(f"Data: {out_data}")
    # ==================================================================================================================

    top_print("", 100)

    fim = time.time()
    print("Time:",fim-inicio)
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())