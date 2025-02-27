from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import httpx
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import xlrd
from async_db import SessionLocal, engine, BaseModel
from models import SpimexTradingResults
import requests
import time 


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



async def process_page(session, page):
    url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"

    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, "lxml")
        data = soup.find("div", class_="accordeon-inner").find_all("div", class_="accordeon-inner__wrap-item")
        
        tasks_pd = []

        for d in data:
            excel = d.find("a", class_="accordeon-inner__item-title link xls")
            date_html = d.find("p")
            date = datetime.strptime(date_html.find("span").text, "%d.%m.%Y")

            if date < datetime(2025, 2, 1):
                continue
            
            if not excel:
                continue
            
            excel_url = "https://spimex.com" + excel.get("href")

            task_pd = asyncio.create_task(url_excel_to_df(excel_url=excel_url, date=date, session=session))
            tasks_pd.append(task_pd)

            # df = await url_excel_to_df(excel_url=excel_url, date=date)   
            # await save_on_db(df=df)
        
        dataframes = await asyncio.gather(*tasks_pd, return_exceptions=True)
        
        tasks_db = [asyncio.create_task(save_on_db(df=df)) for df in dataframes]
        await asyncio.gather(*tasks_db)

        # for df in dataframes:
        #     await save_on_db(df=df)
    

def read_excel(excel_bytes, date):

    df = xlrd.open_workbook(file_contents=excel_bytes)

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

    
async def url_excel_to_df(excel_url, date, session):

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
    
    
    async with session.get(excel_url) as response:

        response.raise_for_status() 
        excel_bytes = await response.read()
        data = read_excel(excel_bytes=excel_bytes, date=date)

    return pd.DataFrame(data, columns=head_db)

async def save_on_db(df):  
    async with SessionLocal() as session:
        data = df.to_dict(orient='records')

        for row in data:
            record = SpimexTradingResults(**row) 
            session.add(record)

        await session.commit()

async def async_main():
    pages = count_pages()
    async with aiohttp.ClientSession() as session:
        # try:
        tasks = []
        for page in range(pages):
            task = asyncio.create_task(process_page(session, page+1))
            tasks.append(task)
            # await process_page(session, page+1)
        await asyncio.gather(*tasks)
        # finally:
        #     await session.close()
            
                    

if __name__ == '__main__':

    start = time.time()

    # asyncio.run(async_main())

    asyncio.get_event_loop().run_until_complete(async_main())

    print("Программа длилась", time.time() - start, "секунд")
