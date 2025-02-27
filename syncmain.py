import requests
from bs4 import BeautifulSoup
from models import Base, SpimexTradingResults
from sync_db import engine, SessionLocal
import requests
import xlrd
import pandas as pd
from datetime import datetime


Base.metadata.create_all(bind=engine)

def count_pages():
    page = 1
    stop = False

    while True:
        if stop:
            break
        
        url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "lxml")
        data = soup.find("div", class_="accordeon-inner").find_all("div", class_="accordeon-inner__wrap-item")

        for d in data:
            date_html = d.find("p")
            date = datetime.strptime(date_html.find("span").text, "%d.%m.%Y")

            if date < datetime(2025, 2, 1):
                stop = True
                break
            break
            
        page += 1
    return page


def process_page(page):
    url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    data = soup.find("div", class_="accordeon-inner").find_all("div", class_="accordeon-inner__wrap-item")

    for d in data:
        excel = d.find("a", class_="accordeon-inner__item-title link xls")
        date_html = d.find("p")
        date = datetime.strptime(date_html.find("span").text, "%d.%m.%Y")

        if date < datetime(2025, 2, 1):
            continue

        if not excel:
            continue
        
        excel_url = "https://spimex.com" + excel.get("href")


        df = url_excel_to_df(excel_url, date)

        save_on_db(df)
   

def parser():
    pages = count_pages()
    for page in range(pages):
        process_page(page=page+1)


def url_excel_to_df(excel_url, date):

    head_db = [
        "exchange_product_id",
        "exchange_product_name",
        "oil_id",
        "delivery_basis_id",
        "delivery_basis_name",
        "delivery_type_id",
        "volume",
        "total",
        "count",
        "date",
        "created_on",
        "updated_on"
        ]
    
    response = requests.get(excel_url)

    data = read_excel(response=response, date=date)

    return pd.DataFrame(data, columns=head_db)


def read_excel(response, date):

    df = xlrd.open_workbook(file_contents=response.content)

    sheet = df.sheet_by_index(0)

    data = []
    save = False
    for i in range(sheet.nrows):
        if "Единица измерения: Метрическая тонна" in sheet.row_values(i)[1]:
            save = True
            continue
        
        if save and len(sheet.row_values(i)[1]) == 11:
            values = sheet.row_values(i)
            if values[14].isdigit():
                if int(values[14]) > 0:
                    data.append([values[1], values[2], values[1][:4], values[1][4:7], 
                    values[3], values[1][-1], values[4], values[5], values[14], date, datetime.utcnow(), datetime.utcnow()])

    return data


def save_on_db(df):
    with SessionLocal() as session:
        data = df.to_dict(orient='records')

        for row in data:
            record = SpimexTradingResults(**row) 
            session.add(record)

        session.commit()


if __name__ == '__main__':
    import time 


    start = time.time()

    parser()

    print("Программа длилась", time.time() - start, "секунд")





    
    



