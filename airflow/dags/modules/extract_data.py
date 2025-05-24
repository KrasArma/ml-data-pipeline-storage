  
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re


def extract_data_product(ids):
    all_data = []
    patterns = {
        "price": r'"price":\s*([^,\n]+)',
        "okrug": r':"([а-яА-ЯёЁa-zA-Z0-9\s\-]+)","type":"okrug"',
        "roomsCount": r'"roomsCount":\s*([^,\n]+)',
        "ceilingHeight": r'"ceilingHeight":\s*"([^,\n]+)"',
        "totalArea": r'"totalArea":\s*"([^,\n]+)"',
        "floorNumber": r'"floorNumber":\s*([^,\n]+)',
        "floorsCount": r'"floorsCount":\s*([^,\n]+)',
        "cargoLiftsCount": r'"cargoLiftsCount":\s*([^,\n]+)',
        "houseMaterialType": r'"houseMaterialType":\s*"([^,\n]+)"'
    }

    for id in ids:
        url = f'https://www.cian.ru/sale/flat/{id}/'
        session = requests.Session()
        response = session.get(url)

        if response.status_code == 200:
            html_content = response.text

            data = {
                "id": id,
                "price": None,
                "okrug": None,
                "roomsCount": None,
                "ceilingHeight": None,
                "totalArea": None,
                "floorNumber": None,
                "floorsCount": None,
                "cargoLiftsCount": None,
                "houseMaterialType": None
            }

            for key, pattern in patterns.items():
                match = re.search(pattern, html_content)
                if match:
                    data[key] = match.group(1).strip('"')
            all_data.append(data)

    return pd.DataFrame(all_data)



def extract_products_list_cian(): 
    url = 'https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&room2=1' 
    session = requests.Session()

    response = session.get(url)
    if response.status_code == 200:
        html_content = response.text

    # price_pattern = r'"price":(\d+)'
    id_pattern = r'"id":(\d+)'

    # prices = re.findall(price_pattern, html_content)
    ids = re.findall(id_pattern, html_content)

    return extract_data_product(set(ids))


def extract_stats_wiki():
    url = 'https://ru.wikipedia.org/wiki/%D0%90%D0%B4%D0%BC%D0%B8%D0%BD%D0%B8%D1%81%D1%82%D1%80%D0%B0%D1%82%D0%B8%D0%B2%D0%BD%D0%BE-%D1%82%D0%B5%D1%80%D1%80%D0%B8%D1%82%D0%BE%D1%80%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%B5_%D0%B4%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5_%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D1%8B'
    session = requests.Session()
    response = session.get(url)

    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        headers = [header.text.strip() for header in table.find_all('th')]
        soup = BeautifulSoup(html_content, 'html.parser')
        data, title = [], []
        for row in soup.find_all('tr'):
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele]) 
            cols = row.find_all('th')
            cols = [ele.text.strip() for ele in cols]
            title.append([ele for ele in cols if ele]) 

        df_areas = pd.concat(
            [
                pd.DataFrame([item[0] for item in title[4:16]], columns=['Area']), 
                pd.DataFrame(data[4:16],columns=title[3][1:11])
            ], axis=1
        ) 
        df_areas.set_index('Area', inplace=True)

        return df_areas






