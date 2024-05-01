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
        contents.append(content)
    # print(contents)

    wether = pd.DataFrame(contents,columns=[title])
    
    saveDir = "WetherStanp"
    if not os.path.isdir(saveDir):
        os.makedirs(saveDir)

    wether.to_csv('WetherStanp\wetherStand.csv')

main()