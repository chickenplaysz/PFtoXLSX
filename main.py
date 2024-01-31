import tabula
import pandas as pd
import requests
import glob
import fitz
from concurrent.futures import ThreadPoolExecutor
import time


start = time.time()

MAX_THREADS = 6
B3_LOCAL_DATABASE = pd.read_excel("runtime/templates/db.xlsx", engine="openpyxl").to_dict()

documents = []
cache = {}


# class for containing PDF docs
class DocRender:
    def __init__(self, path):
        self.path = path
        self.doc = fitz.Document(path)
        self.password = ""

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

        self.data = None
        self.corretora = None


# get needed tickers from stocks

def get_stock_data(nome_ativo, x):
    yfinance = "https://query2.finance.yahoo.com/v1/finance/search"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    params = {"q": nome_ativo, "quotes_count": 1, "country": "United States"}

    res = requests.get(url=yfinance, params=params, headers={'User-Agent': user_agent})
    data = res.json()
    try:
        ativo = data['quotes'][0][x]
        return ativo
    except IndexError:
        return None




def render_pdf(pdf_path: str):
    while True:
        try:
            current_document = DocRender(pdf_path)
            break
        except ValueError:
            print("Senha incorreta\n")
            pass

    if current_document.isocr:
        # TODO: Implement image recognition for inter if possible
        return None

    text = current_document.doc[0].get_text()

    if "avenue" in text.lower():
        # TODO: Create avenue rendering
        return None

    if "xp investimento" in text.lower():
        current_document.corretora = "xp"
        current_document.data = tabula.read_pdf_with_template(
                current_document.path,
                template_path="runtime/templates/xp.json",
                password=current_document.password,
                pages="all",
                pandas_options={"header": ["C/V"]}
        )
    return current_document


def parse_data(doc: DocRender):
    if doc.corretora == "xp":
        tax_table = doc.data[2]

        transactions = doc.data[1]
        transactions = transactions.dropna(how="all", axis=1)
        transactions = transactions.fillna(method="ffill", axis=1)
        transactions.columns = transactions.iloc[0]
        transactions = transactions.drop([0])
        transactions = transactions.loc[:, ~transactions.columns.duplicated(keep="last")].copy()
        transactions = transactions.rename({"Especificação do título": "titulo"}, axis="columns")
        transactions.drop(["Prazo", "Obs. (*)"], axis=1, errors="ignore", inplace=True)

        transactions["titulo"] = transactions["titulo"].apply(lambda t: t[:-1] if get_stock_data(t, "longname") is None else t)

        transactions = pd.DataFrame(transactions)
        transactions["Quantidade"] = pd.to_numeric(transactions["Quantidade"], errors="coerce")


        data_liquidacao = doc.data[0].iloc[1].to_numpy()[0]
        data_pregao = tax_table.loc[tax_table[0].str.contains("quido para", case=False, na=False), 0].str.split(" ", expand=True).iloc[:, 2].reset_index(drop=True)[0]


        cnpj_corretora = "02.332.886/0001-04"
        nome_corretora = "XP INVESTIMENTOS CCTVM S/A"

        df = {
            "tax_table": tax_table,
            "transactions": transactions,
            "data_liquidacao": data_liquidacao,
            "data_pregao": data_pregao,
            "nome_corretora": nome_corretora,
            "cnpj_corretora": cnpj_corretora
        }

        return df

    if doc.corretora == "avenue":
        # TODO: Implement avenue parsing
        return None

    if doc.corretora == "inter":
        # TODO: Implement inter parsing
        return None


def format_data(doc: dict):
    # TODO: all the necessary formating and optimization
        transactions = doc["transactions"]
        print(doc)
        total_quantity = transactions["Quantidade"].sum()
    # TODO: Use @cache to optimize db reading


def main():
    global documents
    parsed_documents = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        documents = list(executor.map(render_pdf, glob.glob("runtime/in/*.pdf")))

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_documents = list(executor.map(parse_data, documents))

    for document in parsed_documents:
        format_data(document)


if __name__ == "__main__":
    main()
    print(time.time() - start)
