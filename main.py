import asyncio
import aiosqlite
from newsqldb import SQLiteDB
import time

query = "CREATE TABLE pessoas (nome TEXT NOT NULL, idade INTEGER)"


class Teste:
    def __init__(self):
        print("Iniciado")
        self.conn = None
        self.connect()

    async def connect(self):
        self.conn = await aiosqlite.connect("main.db")
    def execute(self):
        cursor = self.conn.cursor()
        cursor.execute(query)

    def __del__(self):
        print("Finalizado")


async def main():

    t = Teste()
    t.connect()
    t.execute()

if __name__ == "__main__":
    asyncio.run(main())

