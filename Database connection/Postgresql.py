import psycopg2
from psycopg2 import Error

sum = 1


def createInsertToDb():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="1234",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="attain")

        cursor = connection.cursor()
        # Droping EMPLOYEE table if already exists.
        cursor.execute("DROP TABLE IF EXISTS marketstatistics")
        create_table_query = '''CREATE TABLE marketStatistics
              (
                id   serial primary key,
              Company           varchar(50)    NOT NULL,
              ISN         varchar(50) NOT NULL,
              Volume varchar (10) NOT NULL,
              Last_Trading_Price varchar (10) NOT NULL,
              Change varchar(10) NOT NULL); '''

        # Print PostgreSQL version
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")
        company = "SAFARICOM"
        ISIN = "121321"
        volume = "200"
        ltp = "12"
        change = "0.7"
        postgres_insert_query = '''INSERT INTO marketstatistics (company, isn, volume,last_trading_price,change) VALUES (%s,%s,
        %s,%s,%s) '''
        record_to_insert = (company, ISIN, volume, ltp, change)
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into market table")
    except (Exception, Error) as error:
        print("Error while creating to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


if __name__ == '__main__':
    createInsertToDb()
