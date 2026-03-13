from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# Criando conexão com o banco de dados
engine = create_engine(
    "postgresql+psycopg2://admin:admin@localhost:5432/doctoralia_db"
)



def carrega_tabelas():
    
    try:
        # Carregando tabela 'DIM_ESPECIALIZACAO'
        df_dim_especializacao = pd.read_csv("data/output/dim_especializacao.csv")

        df_dim_especializacao.to_sql(
            "dim_especializacao",
            engine,
            if_exists="append",
            index=False
        )

        # Carregando tabela 'DIM_MEDICO'
        df_dim_medico_com_preco = pd.read_csv("data/output/dim_medico.csv")

        df_dim_medico_sem_preco = df_dim_medico_com_preco[["sk_medico", "sk_especializacao", 
                                                          "id_medico", "nome", "titulo", "atende_remoto", 
                                                          "cidade", "estado"]]

        df_dim_medico_sem_preco.to_sql(
            "dim_medico",
            engine,
            if_exists="append",
            index=False
        )

        # Carregando tabela 'DIM_DATA'
        df_data = pd.read_csv("data/output/dim_data.csv")

        df_data.to_sql(
            "dim_data",
            engine,
            if_exists="append",
            index=False
        )

        # Carregando tabela 'DIM_TIPO_CONSULTA'
        df_tipo_consulta = pd.read_csv("data/output/dim_tipo_consulta.csv")

        df_tipo_consulta.to_sql(
            "dim_tipo_consulta",
            engine,
            if_exists="append",
            index=False
        )
        
        # Criando e carregando a tabela 'FATO CONSULTA'
        df_fato_consulta = pd.DataFrame({
            "sk_consulta": range(1, 150001)
        })

        # Sorteando médicos
        df_fato_consulta["sk_medico"] = df_dim_medico_com_preco["sk_medico"].sample(
            n=len(df_fato_consulta),
            replace=True
        ).values

        # Sorteando datas
        df_fato_consulta["sk_data"] = df_data["sk_data"].sample(
            n=len(df_fato_consulta),
            replace=True
        ).values

        #  Trazendo dados da dimensão médico
        df_fato_consulta = df_fato_consulta.merge(
            df_dim_medico_com_preco[["sk_medico", "atende_remoto", "preco"]],
            on="sk_medico",
            how="left"
        )

        # Gerar tipo da consulta
        df_fato_consulta["tipo_consulta"] = np.where(
            df_fato_consulta["atende_remoto"] == False,
            "presencial",
            np.random.choice([1, 2], len(df_fato_consulta))
        )

        # Converter para SK
        df_fato_consulta = df_fato_consulta.merge(
            df_tipo_consulta,
            on="tipo_consulta",
            how="left"
        )

        # Caso algum valor não encontre correspondência
        df_fato_consulta["sk_tipo_consulta"] = df_fato_consulta["sk_tipo_consulta"].fillna(1)

        # Preço da consulta
        df_fato_consulta["preco_consulta"] = df_fato_consulta["preco"]

        # Gerando nota da consulta
        df_fato_consulta["nota"] = np.round(
            np.random.uniform(0, 5, len(df_fato_consulta)), 1
        )

        # Limpando colunas auxiliares
        df_fato_consulta.drop(
            columns=["tipo_consulta", "atende_remoto", "preco"],
            inplace=True
        )

        # Reordenar colunas
        df_fato_consulta = df_fato_consulta[
            [
                "sk_consulta",
                "sk_medico",
                "sk_data",
                "sk_tipo_consulta",
                "preco_consulta",
                "nota"
            ]
        ]
        
        df_fato_consulta.to_csv('data/output/fato_consulta.csv', index=False)

        df_fato_consulta.to_sql(
            "fato_consulta",
            engine,
            if_exists="append",
            index=False
        )

        print('Integração com o banco de dados feita com sucesso. Dados carregados.')
    except:
        print('Ocorreu algum erro.')
    
