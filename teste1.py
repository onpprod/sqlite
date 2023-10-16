import asyncio
from sqldatabase import SQLiteDB

async def nested():
    return

async def main():
    d = {"comida": "abacate", "quantidade": 10}

    db = SQLiteDB("main.db")
    db.insert_dict("mercadinho",d)
    await db.insert_one(d)
    await db.insert_one_by_collection("mercadinho",d)


    print(await nested())

asyncio.run(main())