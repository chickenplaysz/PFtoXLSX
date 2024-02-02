import tabula
import pandas as pd
import requests
import glob
import fitz
from concurrent.futures import ThreadPoolExecutor
import time
import warnings
import numpy as np
import functools
import swifter


warnings.simplefilter("ignore")
start = time.time()

MAX_THREADS = 6

TRANSACTIONS_TABLE_MODEL = ["negociacao", "cv", "tipo_mercado", "titulo", "obs", "quantidade", "preco", "total", "dc"]
B3_LOCAL_DATABASE = pd.read_excel("runtime/templates/db.xlsx", engine="openpyxl")

BUFFER_SHEET_NAME = "x"
BUFFER_SHEET_PATH = "runtime/buffer_out.xlsx"


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


def write_to_buffer(documents):
    final = pd.DataFrame()

    for document in documents:
        final = final.append(document, ignore_index=True)

    with pd.ExcelWriter(BUFFER_SHEET_PATH) as writer:
        final.to_excel(writer, sheet_name=BUFFER_SHEET_NAME, index=False, header=False)


@functools.lru_cache(maxsize=100, typed=False)
def read_db(ticker, dado) -> tuple:
    match_row = B3_LOCAL_DATABASE[B3_LOCAL_DATABASE['ticker'].swifter.apply(lambda x: str(x) in str(ticker))]

    if not match_row.empty:
        row = match_row.iloc[0]
        return row[dado]

    return "NÃO CADASTRADO NA B3"


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

        data_liquidacao = tax_table.loc[tax_table[0].str.contains("quido para", case=False, na=False), 0].str.split(" ", expand=True).iloc[:, 2].reset_index(drop=True)[0]


        tax_table = tax_table.dropna()
        tax_filter = tax_table[0].str.contains("|".join(["total", "quido para", "valor"]), case=False)
        tax_table = tax_table[~tax_filter]
        tax_table = tax_table.replace(",", ".", regex=True)
        tax_table[1] = pd.to_numeric(tax_table[1])
        total_tax = tax_table[1].sum()

        transactions = doc.data[1]
        transactions = transactions.dropna(how="all", axis=1)
        transactions.columns = transactions.iloc[0]
        transactions["Obs. (*)"] = transactions["Obs. (*)"].fillna("")
        transactions = transactions.fillna(method="bfill", axis=1)

        transactions = transactions.drop([0])
        transactions = transactions.loc[:, ~transactions.columns.duplicated(keep="last")].copy()
        transactions = transactions.rename({"Especificação do título": "titulo"}, axis="columns")
        transactions.drop(["Prazo"], axis=1, errors="ignore", inplace=True)

        for titulo in transactions["titulo"].unique():
            original = titulo
            while True:
                if get_stock_data(titulo, "longname") == None:
                    titulo = titulo[:-1]
                else:
                    transactions["titulo"] = transactions["titulo"].replace(original, titulo)
                    break

        for col in transactions.columns:
            try:
                col*col
                del transactions[col]
            except:
                pass

        transactions["Quantidade"] = pd.to_numeric(transactions["Quantidade"], errors="coerce")

        transactions.columns = TRANSACTIONS_TABLE_MODEL

        total_quantity = transactions["quantidade"].sum()
        transactions = transactions.replace({"preco": {",": "", "\.": ""}, "total": {",": "", "\.": ""}}, regex=True)
        transactions["preco"] = pd.to_numeric(transactions["preco"], errors="coerce")/100
        transactions["total"] = pd.to_numeric(transactions["total"], errors="coerce") / 100
        data_pregao = doc.data[0].iloc[1].to_numpy()[0]
        cnpj_corretora = "02.332.886/0001-04"
        nome_corretora = "XP INVESTIMENTOS CCTVM S/A"


        df = {
            "per_quote_tax": round(total_tax/total_quantity, 2),
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
    transactions = doc["transactions"].reset_index(drop=True)
    formated_data = pd.DataFrame(index=range(transactions.index.size), columns=[
        "data_pregao",
        "data_liquidacao",
        "ticker", "nome_ativo",
        "quantidade",
        "un", "total",
        "despesas_operacionais",
        "preco_medio",
        "irrf_day_trade",
        "irrf_swing_trade",
        "cv",
        "tipo_de_operacao",
        "mercado",
        "nome_corretora",
        "cnpj_corretora",
        "nome_empresa",
        "cnpj_empresa",
    ])

    formated_data["data_pregao"] = formated_data["data_pregao"].map(lambda a: doc["data_pregao"],)
    formated_data["data_liquidacao"] = formated_data["data_liquidacao"].map(lambda a: doc["data_liquidacao"])
    formated_data["nome_ativo"] = transactions["titulo"]
    formated_data["ticker"] = formated_data["nome_ativo"]
    for ativo in formated_data["nome_ativo"].unique():
        formated_data["ticker"] = formated_data["ticker"].replace(ativo, get_stock_data(ativo, "symbol"))

    formated_data["quantidade"] = transactions["quantidade"]
    formated_data["un"] = transactions["preco"]
    formated_data["total"] = transactions["total"]
    formated_data["despesas_operacionais"] = formated_data["quantidade"] * doc["per_quote_tax"]
    formated_data["preco_medio"] = (formated_data["total"] + formated_data["despesas_operacionais"])/transactions["quantidade"]
    formated_data["tipo_de_operacao"] = transactions["obs"].map(lambda a: "DAY_TRADE" if "D" in a else "SWING_TRADE")

    mask_day_trade = formated_data["tipo_de_operacao"] == "DAY_TRADE"
    mask_swing_trade = ~mask_day_trade

    formated_data["irrf_swing_trade"] = np.where(mask_swing_trade, 0, np.where(
        mask_swing_trade & ((formated_data["total"] - formated_data["despesas_operacionais"]) * 0.0005 >= 1),
        round((formated_data["total"] - formated_data["despesas_operacionais"]) * 0.0005, 2), 0))
    formated_data["irrf_day_trade"] = np.where(mask_day_trade, np.where(
        (formated_data["total"] - formated_data["despesas_operacionais"]) * 0.01 >= 1,
        round((formated_data["total"] - formated_data["despesas_operacionais"]) * 0.01, 2), 0), 0)

    del mask_swing_trade
    del mask_day_trade

    formated_data.round(2)
    formated_data["mercado"] = transactions["tipo_mercado"]
    formated_data["nome_corretora"] = formated_data["nome_corretora"].map(lambda a: doc["nome_corretora"])
    formated_data["cnpj_corretora"] = formated_data["cnpj_corretora"].map(lambda a: doc["cnpj_corretora"])
    formated_data["cv"] = transactions["cv"]
    del transactions
    del ativo
    formated_data["nome_empresa"] = formated_data["nome_ativo"].map(lambda a: get_stock_data(a, "shortname"))

    formated_data["cnpj_empresa"] = formated_data["nome_empresa"].map(lambda a: read_db(a, "cnpj"))

    return formated_data


def main():

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        documents = list(executor.map(render_pdf, glob.glob("runtime/in/*.pdf")))

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_documents = list(executor.map(parse_data, documents))

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        formated_documents = list(executor.map(format_data, parsed_documents))

    write_to_buffer(formated_documents)


if __name__ == "__main__":
    main()
    print(time.time() - start)
