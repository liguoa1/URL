import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import datetime
# import re

import pandas as pd

base_url = "http://172.81.240.201:9900/kettle/status/"

response = requests.get(base_url, auth=HTTPBasicAuth('cluster', 'dl4433'))

content = response.content


def parse(html_cont):
    if html_cont is None:
        return None
    else:
        soup = BeautifulSoup(html_cont, 'html.parser', from_encoding='utf-8')
        get_new_data(soup)


def get_new_data(soup):
    title_node = soup.find('title')
    print(title_node.get_text())
    no_need_date = (datetime.datetime.now() + datetime.timedelta(days=-1))
    # 匹配带有class属性的tr标签
    rows = soup.find_all('tr')
    for row in rows:
        tds = row.find_all('td')
        if len(tds) >= 5:
            if tds[2].get_text() == 'Finished':
                finishTime = datetime.datetime.strptime(tds[3].get_text()[0:tds[3].get_text().find('.')]
                                                        , '%Y/%m/%d %H:%M:%S')
                if finishTime > no_need_date:
                    print(tds[1].get_text())
                    print(tds[2].get_text())
                    print(finishTime)


parse(content)

