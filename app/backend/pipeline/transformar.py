from backend.pipeline.extrair import extrai_csv
from backend.schemas.dim_especializacao import schema_especializacao
from backend.schemas.dim_medico import schema_dim_medico
import pandas as pd
import pandera.pandas as pa



def transformando_df(df_doctoralia: pd.DataFrame):
    # Renomeando as colunas para tradução em português
    df_doctoralia.columns = [
        "id", "titulo", "nome", "cidade", "cidade2", "estado", "especializacao",
        "qtd_consultas_avaliadas", "data_ult_avalicao", "atende_remoto", "preco", "url",
        "data_busca_dados"
    ] 

    # Coletando apenas a UF do estado
    df_doctoralia["estado"] = df_doctoralia["estado"].str[-2:].str.upper()

    # Alterando o campo atende_remoto
    df_doctoralia["atende_remoto"] = df_doctoralia["atende_remoto"].astype(int).astype(bool)


def dim_medico(df: pd.DataFrame):
    # Criando a tabela dim_medico
    df_medico = df[["id", "nome", "titulo", "atende_remoto", "cidade", "estado", "qtd_consultas_avaliadas"]]

    # Substituindo o tipo da coluna de Float para Int
    df_medico["qtd_consultas_avaliadas"] = df_medico["qtd_consultas_avaliadas"].astype("Int64")

    # Validação
    try:
        schema_dim_medico.validate(df_medico, lazy=True)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)

    # Salvando o csv
    df_medico.to_csv("data/output/dim_medico.csv", index=False)


def dim_data():
    # Criando o range entre as datas
    datas = pd.date_range(start="2020-01-01", end="2025-12-31")

    # Criando o DF 
    df = pd.DataFrame({"data_completa": datas})

    # Criando colunas dia, mes e ano
    df["dia"] = df["data_completa"].dt.day
    df["mes"] = df["data_completa"].dt.month
    df["ano"] = df["data_completa"].dt.year

    # Salvando o csv
    df.to_csv("data/output/dim_data.csv", index=False)


def dim_especializacao(df: pd.DataFrame):
    # Criando o df com as especializações dos médicos, removendo as duplicadas
    df_especializacao = df[["especializacao"]].drop_duplicates()

    # Substituindo os "-" por espaço nos valores
    df_especializacao["especializacao"] = df_especializacao["especializacao"].str.replace("-", " ")

    # Removendo valor nulo
    for i, valor in df_especializacao["especializacao"].items():
        # Se for NaN ou string vazia / só espaços
        if pd.isna(valor) or str(valor).strip() == "":
            df_especializacao.drop(i, inplace=True) # Deletando a linha

    # Validação
    schema_especializacao.validate(df_especializacao)

    # Salvando o CSV
    df_especializacao.to_csv("data/output/dim_especializacao.csv", index=False)


def dim_tipo_consulta():
    # Criando o df com o tipo da consulta
    df_tipo_consulta = pd.DataFrame({"tipo_consulta": ["remota", "presencial"]})

    # Salvando o CSV
    df_tipo_consulta.to_csv("data/output/dim_tipo_consulta.csv", index=False)

   














    