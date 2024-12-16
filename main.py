from databases import sqlitedatabase
from transform_data import transform_data

if __name__ == '__main__':
    input_database = sqlitedatabase.SQLiteDatabase('crontest3TBT.db')
    input_data, column_names = input_database.fetch('SELECT * FROM cars_car')
    data = transform_data(input_data)

    output_data = sqlitedatabase.SQLiteDatabase('outputcars.db')
    output_data.batch_load('data', data, column_names)


    input_database = sqlitedatabase.SQLiteDatabase('beers20241006 000908.db')
    input_data, column_names = input_database.fetch('SELECT * FROM beers')
    data = transform_data(input_data)

    output_data = sqlitedatabase.SQLiteDatabase('outputbeer.db')
    output_data.batch_load('data', data, column_names)

