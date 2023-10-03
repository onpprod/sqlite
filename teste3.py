from sqldatabase import SQLiteDB
import json

db = SQLiteDB("seubanco.db")

print(db.fetch_data("caral"))
print(db.get_tables_and_columns())