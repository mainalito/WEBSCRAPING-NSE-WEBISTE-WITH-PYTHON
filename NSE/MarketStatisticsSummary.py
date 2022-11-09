import os

import psycopg2
import requests
from bs4 import BeautifulSoup
import pandas as pd
from psycopg2 import Error
import datetime


def fetchMarketSummaryFromNSE(url):
    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')
    date_String = soup.find('div', {"class": "sum_stat_dates"}).find("p").find("b").get_text().split(" ")[3]
    # 07-Oct-2022
    date = datetime.datetime.strptime(date_String,"%d-%b-%Y")

    data = soup.find('div', {"id": "tab-market-statistics-summary"}).find("table",
                                                                          {"class": "nse_table summ_table"}).find(
        "tbody").find_all("tr")

    for d in data:
        result = d.find_all("td")
        # save to postgresql
        print(date)
        InsertToDb(result[0].get_text(), result[1].get_text(), result[2].get_text(),date)

        # date_time = datetime.datetime.now().strftime("%d-%m-%Y")

        # get current working directory
        path = os.getcwd()
        #
        # try: with open('C:\\Users\\User\\Desktop\\ATTAIN\\MARKET SUMMARY DATA\\market-statistics-{}.json'.format(
        # date_time), 'w') as f: f.write("Market Statistics Summary ") f.write(json_records) pass except
        # FileNotFoundError:  # This is skipped if file exists print("FileNotFoundError") except Exception as e:  # This
        # is processed instead print("An exception occurred: ", e) finally: pass


def connectionToDB():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="1234",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="attain")

        cursor = connection.cursor()
        return connection
    except (Exception, Error) as error:
        print("Error while creating to PostgreSQL", error)


def createTable():
    connection = connectionToDB()
    cursor = connection.cursor()
    create_table_query = '''CREATE TABLE statistics_market
          (
            id   serial primary key,
          name           varchar(50)    NOT NULL,
          value         varchar(50) NOT NULL,
          change varchar (20) NOT NULL,
          date DATE NOT NULL
          ); '''

    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully in PostgreSQL ")


def InsertToDb(name, value, change,date):
    global connection, cursor
    try:

        connection = connectionToDB()
        cursor = connection.cursor()
        # # Droping EMPLOYEE table if already exists.
        # cursor.execute("DROP TABLE IF EXISTS marketstatistics")

        postgres_insert_query = '''INSERT INTO statistics_market (name, value, change,date) VALUES (%s,%s,%s,
        %s) '''
        record_to_insert = (name, value, change,date)
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into market table")
    except (Exception, Error) as error:
        print("Error while creating to PostgreSQL", error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


if __name__ == '__main__':
    # createTable()
    fetchMarketSummaryFromNSE("https://www.nse.co.ke/dataservices/market-statistics/")
