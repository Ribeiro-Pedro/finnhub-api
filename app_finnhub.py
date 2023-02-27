import pandas as pd
import requests
from google.oauth2 import service_account

def check_if_valid_data(df: pd.DataFrame) -> bool:
    '''
    Checa se existe valores nulos em determinados campos do dataframe

    Parâmetros:
        df: dataframe recebido pela função load_data, que faz o carregamento dos dados
    
    Retorno:
        booleano falso caso exista alguma incosistência
    '''
    
    # Checa se o dataframe esta vazio
    if df.empty:
        print("\nDataframe empty. Finishing execution")
        return False 
    # Checa nulos
    if df.symbol.empty:
        raise Exception("\nSymbol is Null or the value is empty")
     # Checa nulos
    if df.closed.empty:
        raise Exception("\nPrice is Null or the value is empty")
    # Checa nulos
    if df.datetime.empty:
        raise Exception("\nData is Null or the value is empty")

    return True


def load_data(df, destination_table):
    '''
    Carrega o dataframe Pandas para o banco de dados no Google Big Query

    Parâmetros:
        df: (DataFrame Pandas) Dataframe resultado da função get_candle_data que obtém os dados da API.
        destination_table: (string) Nome da tabela destino.

    Retorno:
        Nenhum.
    '''
  
    # Validação dos dados
    if check_if_valid_data(df):
        print("\nData valid, proceed to Load stage")

    key_path = "C:/Users/pedro/Projetos Python/Projetos/Portifolio/fin_api/api-to-gcp/gbq.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform", 
                                                                                          "https://www.googleapis.com/auth/drive"])
    
    try:
        df.to_gbq(credentials=credentials,destination_table=destination_table, if_exists='replace')
        print('\nData Loaded on Database')

    except:
        print(f"\nFail to load data on database")


def get_candle_data(symbol_list, resolution,start, end,table_name):
    '''
    Obtém os dados que compõem um candlestick da API e chama a função load_data que carrega os dados para o GBQ.

    Parâmetros:
        symbol_list= (list) Lista de symbols(AAPL, MSTS) das ações.
        resolution= (string) Resolução de tempo entre os dados obtidos. Pode ser 1, 5, 15, 30, 60, D, W, M.
        start= (string) Valor inicial do intervalo em UNIX timestamp
        end= (string) Valor final do intervalo em UNIX timestamp

    Retorno:
        Dataframe Pandas com os valores de máximo, mínimo, abertura, fechamento e volume por intervalo de tempo e companhia.
    '''

    
    dados_list = []
    
    for symbol in symbol_list:
        url = f'https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution={resolution}&from={start}&to={end}&token=cfjdtf1r01que34nv07gcfjdtf1r01que34nv080'

        try:
            r = requests.get(url)
            res = r.json()

            for item in range(len(res['c'])):
                dados = {
                    'symbol':symbol,
                    'closed':res['c'][item],
                    'high':res['h'][item],
                    'low':res['l'][item],
                    'open':res['o'][item],
                    'datetime':res['t'][item],
                    'volume':res['v'][item]
                }
                
                dados_list.append(dados)
        except:
            print(f"Error to get {symbol} data")
            exit(1)

    # Transforma dados de data para formato datetime
    candles_df = pd.DataFrame(dados_list)
    candles_df['datetime'] = pd.to_datetime(candles_df['datetime'], unit='s')

    load_data(candles_df,f"projeto_finnhub_api.{table_name}")


symbol_list = ['AAPL','MSFT','GOOG','AMZN','META']

# Obtém dados diários
get_candle_data(symbol_list, 'D', '1546300800', '1672534800',"dayly_candles")
# Obtém dados semanais
get_candle_data(symbol_list, 'W', '1546300800', '1672534800',"weekly_candles")
# Obtém dados mensais
get_candle_data(symbol_list, 'M', '1546300800', '1672534800',"monthly_candles")
