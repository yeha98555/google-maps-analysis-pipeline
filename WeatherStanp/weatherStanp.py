import requests 
from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np

def main():
    url = "https://e-service.cwa.gov.tw/wdps/obs/state.htm"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        print(f"請求失敗，status code: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, "html.parser")

    title = []
    for acticle in soup.select("tr.active th"):
        title.append(acticle.text.strip())
    # print(title)

    contents = []
    for a in soup.select("table tr"):
        content = []
        for acticle in a.select("td"):
            content.append(acticle.text)
        if len(content):
            contents.append(content)

    weather = pd.DataFrame(contents[0:737],columns=[title])
    weather1 = weather[['站號','站名','經度','緯度','城市','地址']]

    saveDir = "WeatherStanp"
    if not os.path.isdir(saveDir):
        os.makedirs(saveDir)

    weather.to_csv('WeatherStanp\weatherStand.csv')
    weather1.to_csv('WeatherStanp\weatherStand1.csv')

main()