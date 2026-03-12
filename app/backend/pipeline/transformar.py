from backend.pipeline.extrair import extrai_csv
from backend.schemas.dim_especializacao import schema_especializacao
from backend.schemas.dim_medico import schema_dim_medico
import pandas as pd
import pandera.pandas as pa


def transformando_df(df_doctoralia: pd.DataFrame):
    # Renomeando as colunas para tradução em português
    df_doctoralia.columns = [
        "id_medico", "titulo", "nome", "cidade", "cidade2", "estado", "especializacao",
        "qtd_consultas_avaliadas", "data_ult_avalicao", "atende_remoto", "preco", "url",
        "data_busca_dados"
    ]

    # Substituindo os "-" por espaços nos valores na coluna especializacao
    df_doctoralia["especializacao"] = df_doctoralia["especializacao"].str.replace("-", " ")

    # Coletando apenas a UF do estado
    df_doctoralia["estado"] = df_doctoralia["estado"].str[-2:].str.upper()

    # Alterando o campo atende_remoto
    df_doctoralia["atende_remoto"] = df_doctoralia["atende_remoto"].astype(int).astype(bool)

    return df_doctoralia


def dim_especializacao(df: pd.DataFrame):
    # Criando o df com as especializações dos médicos, removendo as duplicadas
    df_especializacao = df[["especializacao"]].drop_duplicates()

    # Removendo valor nulo
    for i, valor in df_especializacao["especializacao"].items():
        # Se for NaN ou string vazia / só espaços
        if pd.isna(valor) or str(valor).strip() == "":
            df_especializacao.drop(i, inplace=True) # Deletando a linha
    
    # Criando uma especialização "desconhecida" para os médicos sem especializações informada
    linha_desconhecida = pd.DataFrame({"especializacao": ["desconhecido"]})
    df_especializacao = pd.concat(
        [linha_desconhecida, df_especializacao],
        ignore_index=True
    )

    # Criando surrogate key
    df_especializacao.insert(0, "sk_especializacao", range(0, len(df_especializacao)))

    # Convertando o type de float para int da coluna sk_especializacao
    df_especializacao["sk_especializacao"] = df_especializacao["sk_especializacao"].astype("Int64")

    # Criando o ID
    df_especializacao.insert(1, "id", range(0, len(df_especializacao)))

    # Validação
    schema_especializacao.validate(df_especializacao)

    # Salvando o CSV
    df_especializacao.to_csv("data/output/dim_especializacao.csv", index=False)

    return "Arquivo 'dim_especializacao.csv' criado com sucesso"


def dim_medico(df: pd.DataFrame):
    # Lendo o csv para buscar a sk_especializacao
    df_especializacao = pd.read_csv("data/output/dim_especializacao.csv")

    # Junção dos DF
    df = df.merge(df_especializacao, on="especializacao", how="left")

    # Trocando os valores Naan por 0 e alterando o type de float para int
    df["sk_especializacao"] = df["sk_especializacao"].fillna(0).astype("Int64")

    # Criando a tabela dim_medico
    df_medico = df[["sk_especializacao", "id_medico", "nome", "titulo", "atende_remoto", "cidade", "estado", "preco"]]

    # converter para número
    df_medico["preco"] = pd.to_numeric(df_medico["preco"], errors="coerce")

    # remover preços zero ou negativos
    df_medico.loc[df_medico["preco"] <= 0, "preco"] = None

    # Criando surrogate key
    df_medico.insert(0, "sk_medico", range(1, len(df_medico) + 1))

    # Validação
    try:
       schema_dim_medico.validate(df_medico, lazy=True)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)

    # Salvando o csv
    df_medico.to_csv("data/output/dim_medico.csv", index=False)

    return "Arquivo 'dim_medico.csv' criado com sucesso"


def dim_data():
    # Criando o range entre as datas
    datas = pd.date_range(start="2020-01-01", end="2025-12-31")

    # Criando o DF 
    df = pd.DataFrame({"data_completa": datas})

    # Criando colunas dia, mes e ano
    df["dia"] = df["data_completa"].dt.day
    df["mes"] = df["data_completa"].dt.month
    df["ano"] = df["data_completa"].dt.year

    # Criando surrogate key
    df.insert(0, "sk_data", range(1, len(df) + 1))

    # Salvando o csv
    df.to_csv("data/output/dim_data.csv", index=False)

    return "Arquivo 'dim_data.csv' criado com sucesso"


def dim_tipo_consulta():
    # Criando o df com o tipo da consulta
    df_tipo_consulta = pd.DataFrame({"tipo_consulta": ["remota", "presencial"]})

    # Criando surrogate key
    df_tipo_consulta.insert(0, "sk_tipo_consulta", range(1, len(df_tipo_consulta) + 1))

    # Criando o ID
    df_tipo_consulta.insert(1, "id", range(1, len(df_tipo_consulta) + 1))

    # Salvando o CSV
    df_tipo_consulta.to_csv("data/output/dim_tipo_consulta.csv", index=False)

    return "Arquivo 'dim_tipo_consulta.csv' criado com sucesso"

    
        

if __name__ == "__main__":
    df = extrai_csv("data/input")
    df_transformado = transformando_df(df)
    print(dim_especializacao(df_transformado))
    print(dim_medico(df_transformado))
    print(dim_data())
    print(dim_tipo_consulta())



   














    