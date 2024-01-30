import tabula
import pandas as pd
import requests
import glob
import fitz
import numpy as np

TICKS = ["N1", "N2", "NM", "ON", "PN"]

documents = []


# class for containing PDF docs
class CurrentDoc:
    def __init__(self, path):
        self.path = path
        self.doc = fitz.Document(path)

        if self.doc.needs_pass:
            passcode = input(f"A nota {path} é protegida por senha\nSenha: ")
            self.doc.authenticate(password=passcode)
            self.password = passcode
        else:
            self.password = None

        if self.doc[0].get_text():
            self.isocr = False
        else:
            self.isocr = True

        self.table = None


# get needed tickers from stocks

def get_ticker(nome_ativo):
    yfinance = "https://query2.finance.yahoo.com/v1/finance/search"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    params = {"q": nome_ativo, "quotes_count": 1, "country": "United States"}

    res = requests.get(url=yfinance, params=params, headers={'User-Agent': user_agent})
    data = res.json()

    codigo_ativo = data['quotes'][0]['symbol']
    return codigo_ativo


test_pdf = 'in/xp senha.pdf'

doc = CurrentDoc(test_pdf)

if doc.isocr:
    pass

else:
    for page in doc.doc:
        text = page.get_text()
        if "xp investimentos" in text.lower():
            doc.table = tabula.read_pdf_with_template(
                doc.path,
                template_path="templates/xp.json",
                password=doc.password,
                pages="all",
                pandas_options={"header": ["Negociação", "C/V", "Tipo mercado"]}
            )


            # removes headers and turn to list
            doc.table[1] = doc.table[1].drop(0).reset_index(drop=True, col_level=0)

            parsed_data = pd.DataFrame(columns=range(19))
            parsed_data.loc[len(parsed_data)] = range(19)
            print(parsed_data)

            for index in range(len(doc.table[1])):
                ativo = doc.table[1].iloc[index].dropna().reset_index(drop=True)
                while True:
                    if ativo[3][-2:] in TICKS:
                        ativo[3] = ativo[3][:-2].rstrip()

                    else:
                        break




        if "interasdsasd" in text.lower():
            doc.table = tabula.read_pdf_with_template(
                doc.path,
                template_path="",
                password=doc.password,
                pages="all"
            )
            print(doc.table)
