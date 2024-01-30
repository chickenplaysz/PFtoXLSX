import tabula
import pandas as pd
import requests
import glob
import fitz
import numpy as np
import time

timer = time.time()
TICKS = ["N1", "N2", "NM", "ON", "PN", "ES", "ED", "EJ", "EB", "ER"]

documents = []
cache = {}


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

def get_ticker(nome_ativo, x):
    yfinance = "https://query2.finance.yahoo.com/v1/finance/search"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    params = {"q": nome_ativo, "quotes_count": 1, "country": "United States"}

    res = requests.get(url=yfinance, params=params, headers={'User-Agent': user_agent})
    data = res.json()

    ativo = data['quotes'][0][x]
    return ativo


test_pdf = ('runtime/in/nota xp padrao.pdf')

doc = CurrentDoc(test_pdf)
db = pd.read_excel("runtime/templates/db.xlsx")
parsed_data = pd.DataFrame(columns=range(19))

maxQuant = 0
dataPregao = ""
dataLiq = ""


if doc.isocr:
    pass

else:
    for page in doc.doc:
        text = page.get_text()
        if "xp investimentos" in text.lower():
            doc.table = tabula.read_pdf_with_template(
                doc.path,
                template_path="runtime/templates/xp.json",
                password=doc.password,
                pages="all",
                pandas_options={"header": ["Negociação", "C/V", "Tipo mercado"]}
            )

            # removes headers and turn to list
            doc.table[1] = doc.table[1].drop(0).reset_index(drop=True, col_level=0)

            dataPregao = str(doc.table[0][0][1])
            dataLiq = [x.split(" ")[2] for x in doc.table[2][0] if "quido para" in str(x)][0]


            range_of_lookout = 6
            for i in range(3):
                try:
                    a = int(doc.table[1][range_of_lookout][1])
                except ValueError:
                    range_of_lookout += 1

            if maxQuant == 0:
                for qty in doc.table[1][range_of_lookout]:
                    maxQuant += int(qty)




            for index in range(len(doc.table[1])):
                ativo = doc.table[1].iloc[index].dropna().reset_index(drop=True)

                while True:
                    try:
                        a = get_ticker(ativo[3], "longname")

                    except:
                        ativo[3] = ativo[3][:-1]

                    else:
                        break

                codNegociacao = get_ticker(ativo[3], "symbol")
                nomePregao = ativo[3]
                quantidade = ativo[5]
                valorUn = ativo[6]
                valorTotal = ativo[7]
                irrfdt = 0
                irrfst = 0
                cv = ativo[1]
                dc = ativo[range_of_lookout]
                mercado = ativo[2]
                corretora = "XP Investimentos"
                cnpjCorretora = "02.332.886/0001-04"
                empresa = get_ticker(ativo[3], "longname")
                try:
                    cnpjEmpresa = cache[codNegociacao.replace(".SA", "")]
                except KeyError:
                    cnpjEmpresa = db.loc[db['Ticker'].str.contains(f"{codNegociacao.replace('.SA', '')}")]
                    cache[codNegociacao] = cnpjEmpresa





        if "interasdsasd" in text.lower():
            doc.table = tabula.read_pdf_with_template(
                doc.path,
                template_path="",
                password=doc.password,
                pages="all"
            )
            print(doc.table)
print(time.time() - timer)