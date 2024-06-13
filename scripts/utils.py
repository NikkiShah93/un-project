import requests
from bs4 import BeautifulSoup
import json
import os

CWD = os.getcwd()
PARENTDIR = os.path.dirname(os.getcwd()) #os.path.dirname(os.path.abspath(__file__))

## the api doesn't allow access to this dataset directly
## so we have to use page numbers to scrape the data
## the api doesn't allow access to this dataset directly
## so we have to use page numbers to scrape the data
def get_data(url, table_name="table"):
    i = 1
    data = {}
    response = requests.get(url.replace("&v=pagenum", str(i)))
    soup = BeautifulSoup(response.content, 'html.parser')
    page_count = soup.find("span", {"id":"spanPageCountB"}).text
    print(f"Staring to get data for {table_name}")
    while response.ok and i <= int(page_count):
        # print("On page: ", i)
        # print(url.replace("=pagenum", f"={i}"))
        for tb in BeautifulSoup(response.content, 'html.parser').find_all("div", {"class":"DataContainer"}):
            if i == 1:
                data = {x.text:[] for x in tb.find_all('th') if x is not None}
            for row in tb.find_all('tr'):
                if row.find('td') is not None:
                    vals = [v.text for v in row.find_all('td') if v is not None]
                    for pos, k in enumerate(data):
                        if len(vals) > pos:
                            data[k].append(vals[pos])
        i += 1
        response = requests.get(url.replace("=pagenum", f"={i}"))
    return data


def get_file(path, parent=True):
    if parent:
        full_path = PARENTDIR + path
    else:
        full_path = CWD + path
    with open(full_path, 'r') as f:
        file = json.load(f)
    return file