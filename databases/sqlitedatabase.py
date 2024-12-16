import sqlite3


class SQLiteDatabase:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def fetch(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall(), [c[0] for c in self.cursor.description]

    def batch_load(self, table_name, data, column_names):
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(column_names)})')
        for row in data:
            placeholders = ', '.join('?' * len(row))
            values = row
            cmd = f'INSERT INTO {table_name} {column_names} VALUES {values}'
            self.cursor.execute(f'INSERT INTO {table_name} {tuple(column_names)} VALUES {tuple(values)}')
        self.connection.commit()

    def close(self):
        self.connection.close()