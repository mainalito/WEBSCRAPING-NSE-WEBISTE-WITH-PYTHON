import time
import datetime
import pandas as pd
from bs4 import BeautifulSoup
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
    browser.find_element(By.LINK_TEXT, 'Bonds Statistics').click()
    b_stat_dates = browser.find_element(By.XPATH,
                                        '/html/body/div[1]/div/div[3]/div[2]/div/div/div[1]/div[2]/div/div/div/div/div/div[3]/div/div[1]/div[2]/p[1]/b').text.split(
        " ")[3]
    date = datetime.datetime.strptime(b_stat_dates, "%d-%b-%Y")
    time.sleep(1)
    select = Select(browser.find_element(By.ID, "bonds_cc_dd_sel"))
    nbritems = len(select.options)
    dfs = []
    for i in range(1, nbritems):
        select = Select(browser.find_element(By.ID, "bonds_cc_dd_sel"))
        txt = select.options[i].text
        select.select_by_index(i)
        time.sleep(2)
        html = browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        divElemm = soup.find('div', {"id": "bonds_dd_content"})
        table = divElemm.find('table', {"class": "table nsetable"})
        equity_all_stats_df = pd.read_html(str(table))[0]
        equity_all_stats_df["Year Bonds"] = txt
        equity_all_stats_df["date"] = date
        dfs.append(equity_all_stats_df)
    equity = pd.concat(dfs)

    equity.to_csv("bond.csv", index=False)

    browser.quit()


if __name__ == '__main__':
    fetchDataFromNSE("https://www.nse.co.ke/dataservices/market-statistics/")
