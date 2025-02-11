import io
import requests
from bs4 import BeautifulSoup
from pars import url_excel_to_df, save_on_db
from models import Base
from database import engine
from datetime import datetime

Base.metadata.create_all(bind=engine)
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
        excel = d.find("a", class_="accordeon-inner__item-title link xls")
        date_html = d.find("p")
        date = datetime.strptime(date_html.find("span").text, "%d.%m.%Y")

        if date < datetime(2023, 1, 1):
            stop = True
            break
        if not excel:
            continue
        
        excel_url = "https://spimex.com" + excel.get("href")


        df = url_excel_to_df(excel_url, date)

        save_on_db(df)
        

    
    page += 1





    
    



