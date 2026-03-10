import os
import pandas as pd


def extrai_csv(caminho: str):
    """
    Função para extrair um csv

    :param caminho: Caminho da pasta onde está o csv
    :type caminho: Str
    :return: Dataframe
    """

    #Extraindo o arquivo csv
    doctoralia_csv = os.path.join(caminho, '202210_doctoralia_br.csv')
    df_doctoralia = pd.read_csv(doctoralia_csv)


    return df_doctoralia




