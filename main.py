import tabula
import pandas as pd
import requests
import glob
import fitz


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

test_pdf = 'in/nota xp padrao.pdf'

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
            # removes headers
            doc.table[2] = doc.table[2].iloc[1:]

            # get atainable data before parsing
            df = pd.DataFrame(columns=range(19))
            dataPreg = doc.table[0][0][1]
            dataLiq = doc.table[5][0][0].split(" ")[2]
            cnpjCorretora = doc.table[1][0][0].split(" ")[1]

            for i in doc.table[2]:
                ativo = doc.table[2].iloc[i]
                if "ON ED NM" in ativo[4]:
                    ativo[4] = str(ativo[4]).replace("ON ED NM", "")
                entry = [
                    None,
                    dataPreg,
                    dataLiq,
                    get_ticker(ativo[4]),  # ticker
                    ativo[4],  # nome
                    ativo[7],  # quantidade
                    ativo[8],  # preco un
                    ativo[9],  # preco total

                ]
                print(entry)

                df.loc[len(df)] = entry




        if "banco inter" in text.lower():
            doc.doc.get
            doc.table = tabula.read_pdf_with_template(
                doc.path,
                template_path="",
                password=doc.password,
                pages="all"
            )
