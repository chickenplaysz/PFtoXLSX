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
import os
import swifter
import random as rnd
from forex_python.converter import CurrencyRates
import sys
import traceback

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By
import pyautogui


warnings.simplefilter("ignore")
start = time.time()

MAX_THREADS = 6

TRANSACTIONS_TABLE_MODEL = ["negociacao", "cv", "tipo_mercado", "titulo", "obs", "quantidade", "preco", "total", "dc"]
B3_LOCAL_DATABASE = pd.read_excel("runtime/templates/db.xlsx", engine="openpyxl")

BUFFER_SHEET_NAME = "x"
BUFFER_SHEET_PATH = "runtime/buffer_out.xlsx"

passwords = []

# class for containing PDF docs
class DocRender:
    global passwords
    def __init__(self, path):
        self.path = path
        if type(path) == str:
            self.doc = fitz.Document(path)
        else:
            self.doc = path
            self.path = path.name

        self.password = ""

        if self.doc.needs_pass:
            for i, passcode in enumerate(passwords):
                try:
                    self.doc.authenticate(password=passcode)
                    self.password = passcode
                    del passwords[i]
                except ValueError:
                    pass
            else:
                if not self.password:
                    raise ValueError("Uma ou mais senhas estão incorretas ou faltantes")

        else:
            self.password = None

        self.data = None
        self.corretora = None

def except_and_exit(exc_type, exc_value, tb):

    traceback.print_exception(exc_type, exc_value, tb)
    print("Pressione qualquer tecla para sair...")
    input()
    sys.exit(-1)

def ocr_check():
    opt = webdriver.ChromeOptions()
    opt.add_experimental_option("prefs", {
        "download.default_directory": f"{os.getcwd()}\\runtime\\in"
    })
    driver = webdriver.Chrome(options=opt)
    def selenium_ocr(path):
        driver.get("https://www.ilovepdf.com/pt/ocr-pdf")
        driver.find_element(By.ID, "pickfiles").click()
        time.sleep(6)
        pyautogui.write(os.path.abspath(path))
        pyautogui.press("enter")

        process_task = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "processTask"))
        )
        process_task.click()
        del process_task
        time.sleep(8)

        download = WebDriverWait(driver, 100).until(
            EC.presence_of_element_located((By.ID, "pickfiles"))
        )
        download.click()
        del download
        time.sleep(2)


    paths = list(glob.iglob("runtime\\in\\*.pdf"))
    for path in paths:
        doc = fitz.Document(path)
        try:
            if not doc[0].get_text():
                del doc
                selenium_ocr(path)
                os.remove(path)
        except ValueError:
            pass


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
        final = pd.concat([final, document], ignore_index=True)

    with pd.ExcelWriter(BUFFER_SHEET_PATH, engine="openpyxl") as writer:
        final.to_excel(writer, sheet_name=BUFFER_SHEET_NAME, index=False, header=False)


@functools.lru_cache(maxsize=100, typed=False)
def read_db(ticker) -> tuple:
    for row in B3_LOCAL_DATABASE.itertuples():
        if row.ticker in ticker:
            return row.cnpj

    return "NÃO CADASTRADO NA B3",


def render_pdf(pdf_path: str):
    print("rendering...")

    current_document = DocRender(pdf_path)

    text = current_document.doc[0].get_text()

    if "inter dtvm" in text.lower():
        current_document.corretora = "inter"
        current_document.data = tabula.read_pdf_with_template(
            current_document.path,
            template_path="runtime/templates/inter.json",
            password=current_document.password,
            pages="all",
            pandas_options={"header": ["C/V"]}
        )
        return current_document

    if "avenue" in text.lower():
        child_pages = []
        data = pd.DataFrame()
        current_document.corretora = "avenue"


        current_document.doc.delete_page(0)
        for index, page in enumerate(current_document.doc.pages()):
            temp_document = fitz.Document()
            temp_document.insert_pdf(current_document.doc, from_page=index, to_page=index, start_at=0)
            temp_document[0].number = 0
            child_pages.append(temp_document)

        child_pages = [DocRender(page) for page in child_pages]

        for index, page in enumerate(child_pages):
            page.corretora = "avenue"
            page.path = f"runtime\\in\\{rnd.randint(0, 100000)}_{index}.pdf"
            page.doc.save(page.path)

            if len(child_pages) == 1:
                for i in range(6):

                    current_table = tabula.read_pdf_with_template(
                        page.path,
                        template_path=f"runtime/templates/avenue_single_{i+1}.json",
                        password=page.password,
                        pages="all",
                        pandas_options={"header": ["COMM", "Tran Fee", "Fees", "Number", "Net Amount", "Trade#"]}
                    )[0]


                    if current_table.swifter.progress_bar(False).apply(lambda a: True if a.isin(["SUMMARY FOR CURRENT TRADE DATE:"]).any() else False).any():
                        break
                    best = current_table
                page.data = best
                os.remove(page.path)

            elif index == 0:
                page.data = tabula.read_pdf_with_template(
                    page.path,
                    template_path="runtime/templates/avenue_first.json",
                    password=page.password,
                    pages="all",
                    pandas_options={"header": ["Type"]}
                )[0]
                os.remove(page.path)


            elif index == len(child_pages) - 1:
                for i in range(6):

                    current_table = tabula.read_pdf_with_template(
                        page.path,
                        template_path=f"runtime/templates/avenue_last_{i+1}.json",
                        password=page.password,
                        pages="all",
                        pandas_options={"header": ["COMM", "Tran Fee", "Fees", "Number", "Net Amount", "Trade#"]}
                    )[0]
                    if current_table.swifter.progress_bar(False).apply(lambda a: True if a.isin(["SUMMARY FOR CURRENT TRADE DATE:"]).any() else False).any():
                        break
                    best = current_table
                page.data = best
                os.remove(page.path)

            else:
                page.data = tabula.read_pdf_with_template(
                    page.path,
                    template_path="runtime/templates/avenue_normal.json",
                    password=page.password,
                    pages="all",
                    pandas_options={"header": ["Type"]}
                )[0]
                os.remove(page.path)



            page.data = page.data.dropna(axis=1, how="all")
            page.data.columns = page.data.iloc[0]
            page.data = page.data.drop(index=0)
            data = pd.concat([data, page.data], ignore_index=True)

        current_document.data = data.reset_index(drop=True)
        del data
        del current_table
        del child_pages
        del best
        del temp_document

        return current_document

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

    return None


def parse_data(doc: DocRender):
    print("parsing...")

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
            "cnpj_corretora": cnpj_corretora,
            "corr": "xp"
        }

        return df

    if doc.corretora == "avenue":

        doc.data = pd.merge(doc.data.iloc[::4, :].reset_index(), doc.data.iloc[1::4, :].reset_index(), left_index=True, right_index=True) \
        .dropna(how="all", axis=1).drop(["index_x", "T_x", "P_x", "index_y", "Number_y", "Net Amount Trade#_y"], axis=1) \

        doc.data[["type", "bs", "trade date"]] = doc.data["Type B/S Trade Date_x"].str.split(" ", expand=True)
        doc.data[["net amount", "trade#"]] = doc.data["Net Amount Trade#_x"].str.split(" ", expand=True)
        doc.data[["dsc", "name"]] = doc.data["Type B/S Trade Date_y"].str.split(":", expand=True)
        doc.data = doc.data.drop([
            "type",
            "Type B/S Trade Date_x",
            "Type B/S Trade Date_y",
            "Net Amount Trade#_x",
            "trade#",
            "dsc",
            "Fees_y",
            "Number_x"
        ], axis=1)
        doc.data.columns = [
            "data_liquidacao",
            "quantidade",
            "ticker",
            "un",
            "total",
            "corretagem",
            "taxa_transacao",
            "taxa_outras",
            "cv",
            "data_pregao",
            "preco_medio",
            "nome"
        ]
        doc.data["cv"] = doc.data["cv"].map(lambda a: "C" if "B" in a else "V")
        doc.data["corretagem"] = pd.to_numeric(doc.data["corretagem"])
        doc.data["taxa_transacao"] = pd.to_numeric(doc.data["taxa_transacao"])
        doc.data["taxa_outras"] = pd.to_numeric(doc.data["taxa_outras"])

        return {
            "transactions": doc.data,
            "corr": "avenue"
        }

    if doc.corretora == "inter":
        tax_table = doc.data[2]

        data_liquidacao = tax_table.loc[tax_table[0].str.contains("quido para", case=False, na=False), 0].str.split(" ", expand=True).iloc[:, 2].reset_index(drop=True)[0]
        data_pregao = doc.data[0]
        data_pregao = data_pregao[1][0].split(" ")[1]

        tax_table = tax_table.drop([0, 3, 7, 12],axis=0)
        tax_table = tax_table.drop(2, axis=1)
        total_tax = pd.to_numeric(tax_table[1].astype(str).str.replace(",", ".", regex=True)).sum()

        transactions = doc.data[1]
        transactions.columns = [
            "bolsa",
            "cv",
            "tipo_mercado",
            "titulo",
            "obs",
            "quantidade",
            "un",
            "total",
            "tipo_de_operacao",
        ]
        transactions = transactions.drop(0)
        transactions = transactions.drop("bolsa", axis=1)
        transactions = transactions.drop(transactions[transactions["titulo"] == "SubTotal :"].index)
        transactions = transactions.fillna("")

        total_quantity = pd.to_numeric(transactions["quantidade"]).sum()
        per_quote_tax = round(total_tax/total_quantity, 2)

        for titulo in transactions["titulo"].unique():
            original = titulo
            while True:
                if get_stock_data(titulo, "longname") == None:
                    titulo = titulo[:-1]
                else:
                    transactions["titulo"] = transactions["titulo"].replace(original, titulo)
                    break

        for col in ["un", "total"]:
            transactions[col] = pd.to_numeric(transactions[col].str.replace('[,.]', '', regex=True)) / 100
            transactions[col] = transactions[col].round(2)

        transactions["quantidade"] = pd.to_numeric(transactions["quantidade"].astype(str).str.replace('\.', '', regex=True))

        return {
            "data_pregao": data_pregao,
            "data_liquidacao": data_liquidacao,
            "per_quote_tax": per_quote_tax,
            "transactions": transactions,
            "cnpj_corretora": "8.945.670/0001-46",
            "nome_corretora": "Inter DTVM Ltda.",
            "corr": "inter",
        }


def format_data(doc: dict):
    print("formating...")

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

    if doc["corr"] == "xp":

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
        formated_data["nome_empresa"] = formated_data["nome_ativo"].map(lambda a: get_stock_data(a, "longname"))

        formated_data["cnpj_empresa"] = formated_data["ticker"].map(lambda a: read_db(a))

        return formated_data

    if doc["corr"] == "avenue":
        cr = CurrencyRates()
        formated_data["data_pregao"] = transactions["data_pregao"]
        formated_data["data_liquidacao"] = transactions["data_liquidacao"]
        formated_data["ticker"] = transactions["ticker"]
        formated_data["nome_ativo"] = transactions["nome"]
        formated_data["quantidade"] = transactions["quantidade"]
        formated_data["un"] = pd.to_numeric(transactions["un"]).round(3)
        formated_data["un"] = formated_data["un"].map(lambda a: cr.convert("USD", "BRL", float(a))).replace(",", "\.", regex=True)
        formated_data["total"] = transactions["total"]
        formated_data["total"] = formated_data["total"].map(lambda a: cr.convert("USD", "BRL", float(a))).replace(",", "\.", regex=True)
        transactions["despesas_operacionais"] = transactions["corretagem"] + transactions["taxa_transacao"] + transactions["taxa_outras"]

        columns_to_convert = ["un", "total", "despesas_operacionais", "preco_medio"]

        # Perform currency conversion first
        formated_data[columns_to_convert] = cr.convert("USD", "BRL", transactions[columns_to_convert].astype(float))

        # Convert to numeric, round, and convert to string
        formated_data[columns_to_convert] = formated_data[columns_to_convert].round(3).astype(str)

        # Remove periods and commas
        formated_data[columns_to_convert] = formated_data[columns_to_convert].replace(["\.", ","], "", regex=True)

        # Convert back to float and divide by 100
        formated_data[columns_to_convert] = formated_data[columns_to_convert].astype(float) / 100


        formated_data = formated_data.assign(
            irrf_day_trade=0,
            irrf_swing_trade=0,
            tipo_de_operacao="-",
            mercado="-",
            nome_corretora="AVENUE SECURITIES LLC",
            cnpj_corretora="61.384.004/0001-05",
            cnpj_empresa="-"
        )

        formated_data["cv"] = transactions["cv"]
        formated_data["nome_empresa"] = transactions["ticker"].map(lambda a: get_stock_data(a, "longname"))

        return formated_data

    if doc["corr"] == "inter":

        formated_data = formated_data.assign(
            data_pregao=doc["data_pregao"],
            data_liquidacao=doc["data_liquidacao"],
            cnpj_corretora=doc["cnpj_corretora"],
            nome_corretora=doc["nome_corretora"]
        )

        formated_data["ticker"] = transactions["titulo"]
        formated_data["nome_ativo"] = transactions["titulo"].map(lambda a: get_stock_data(a, "shortname"))
        formated_data["quantidade"] = pd.to_numeric(transactions["quantidade"])
        formated_data["un"] = transactions["un"]
        formated_data["total"] = transactions["total"]
        formated_data["despesas_operacionais"] = pd.to_numeric(round(formated_data["quantidade"] * doc["per_quote_tax"], 3))
        formated_data["preco_medio"] = (formated_data["total"].astype(float) + formated_data["despesas_operacionais"].astype(float))
        formated_data["preco_medio"] = formated_data["preco_medio"]/formated_data["quantidade"]
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

        formated_data["cv"] = transactions["cv"]
        formated_data["mercado"] = transactions["tipo_mercado"]
        formated_data["nome_empresa"] = formated_data["nome_ativo"].map(lambda a: get_stock_data(a, "longname"))
        formated_data["cnpj_empresa"] = formated_data["ticker"].map(lambda a: read_db(a))


        return formated_data



def main():

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        documents = list(executor.map(render_pdf, glob.glob("runtime/in/*.pdf")))


    for i, d in enumerate(documents):
        if d == None:
            del documents[i]

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_documents = list(executor.map(parse_data, documents))

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        formated_documents = list(executor.map(format_data, parsed_documents))

    write_to_buffer(formated_documents)


if __name__ == "__main__":
    sys.excepthook = except_and_exit
    passwords = input(
        "Seus documentos possuem senha?\n"
        "Se sim, digite-as abaixo em qualquer ordem separadas por ';' (xxxx;yyyy;zzzz)\n"
        "Senhas:  "
    ).split(";")

    print("Os erros referenciando 'jpype' a seguir são apenas avisos.\nIgnore-os, seus arquivos estão sendo lidos normalmente!")

    ocr_check()
    main()

    print("Arquivos\nTempo de execução:", time.time() - start)
    input("Pressione qualquer tecla para sair...")
