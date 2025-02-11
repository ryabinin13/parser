import requests
import xlrd
import pandas as pd
from datetime import datetime
from database import SessionLocal
from models import SpimexTradingResults

def url_excel_to_df(excel_url, date):

    head = [
    "Код Инструмента", "Наименование Инструмента", "Базис поставки", 
    "Объем Договоров в единицах измерения", "Объем Договоров, руб.",
    "Количество Договоров, шт.", "Дата"
    ]
    
    response = requests.get(excel_url)

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
                    data.append([values[1], values[2], values[3], values[4], values[5], values[14], date])


    return pd.DataFrame(data, columns=head)


def save_on_db(df):
    for index, row in df.iterrows():
        sp = SpimexTradingResults(
            exchange_product_id = row['Код Инструмента'],
            exchange_product_name = row['Наименование Инструмента'],
            oil_id = row['Код Инструмента'][:4],
            delivery_basis_id = row['Код Инструмента'][4:7],
            delivery_basis_name = row['Базис поставки'],
            delivery_type_id = row['Код Инструмента'][-1],
            volume = row['Объем Договоров в единицах измерения'],
            total = row['Объем Договоров, руб.'],
            count = row['Количество Договоров, шт.'],
            date = row['Дата'],
            created_on = datetime.utcnow(),
            updated_on = datetime.utcnow()
        )
        with SessionLocal() as db:
            db.add(sp)
            db.commit()