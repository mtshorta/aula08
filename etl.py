import pandas as pd
import os
import glob


#Transformando JSON em DF e concatenando JSONs de uma pasta
def extrair_dados_e_consolidar(path:str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return(df_total)


#Transformação de dados: Adição de uma colunas"
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda']
    return df

#Carregando os dados do pandas para CSV
def carregar_dados(df: pd.DataFrame, format_saida: list):
    """
    Informa o formato de saída que desejamos salvar se csv, parquet ou os 2
    """
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv('dados.csv', index=False)
        if formato == 'parquet':
            df.to_parquet('dados.parquet', index = False)



#Agora tudo junto
def pipeline_calcular_kpi_de_vendas_consolidado(pasta:str, formato_de_saida:list):
    data_frame = extrair_dados_e_consolidar(path = pasta)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    formato_de_saida = ['csv', 'parquet']
    carregar_dados(data_frame_calculado, formato_de_saida)   


