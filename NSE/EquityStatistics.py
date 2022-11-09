import json
import time
import datetime

import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from psycopg2 import Error
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select


def fetchDataFromNSE(url):
    PATH = "C:/Users/User/Downloads/chromedriver_win32 (1)/chromedriver.exe"
    s = Service(PATH)
    browser = webdriver.Chrome(service=s)

    browser.get(url)
    # click the equity statistics link
    browser.find_element(By.LINK_TEXT, 'Equity Statistics').click()
    time.sleep(1)
    stat_date = browser.find_element(By.XPATH,
                                     '/html/body/div[1]/div/div[3]/div[2]/div/div/div[1]/div[2]/div/div/div/div/div/div[2]/div/div[1]/div[2]/p[1]/b').text.split(
        " ")[3]
    # 07-Oct-2022
    date = datetime.datetime.strptime(stat_date, "%d-%b-%Y")
    select = Select(browser.find_element(By.XPATH, '/html/body/div[1]/div/div[3]/div[2]/div/div/div[1]/div['
                                                   '2]/div/div/div/div/div/div[2]/div/div[1]/div[1]/form/select'))
    nbritems = len(select.options)
    companies = []
    dfs = []
    for i in range(1, nbritems):
        select = Select(browser.find_element(By.XPATH, '/html/body/div[1]/div/div[3]/div[2]/div/div/div[1]/div['
                                                       '2]/div/div/div/div/div/div[2]/div/div[1]/div[1]/form/select'))
        txt = select.options[i].text
        companies.append(txt)
        select.select_by_index(i)
        time.sleep(2)
        html = browser.page_source

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {"class": "table nsetable"})
        equity_all_stats_df = pd.read_html(str(table))[0]
        equity_all_stats_df["Sector"] = txt
        equity_all_stats_df["date"] = date

        dfs.append(equity_all_stats_df)
    equity = pd.concat(dfs)

    equity.to_csv("equity-sectors.csv", index=False)

    browser.quit()


if __name__ == '__main__':
    fetchDataFromNSE("https://www.nse.co.ke/dataservices/market-statistics/")
