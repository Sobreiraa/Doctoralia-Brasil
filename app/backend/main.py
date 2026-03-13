from backend.pipeline.extrair import extrai_csv
from backend.pipeline.transformar import transformando_df, dim_especializacao, dim_medico, dim_data, dim_tipo_consulta
from backend.pipeline.carregar import carrega_tabelas


if __name__ == "__main__":
    df = extrai_csv("data/input")
    df_transformado = transformando_df(df)
    print(dim_especializacao(df_transformado))
    print(dim_medico(df_transformado))
    print(dim_data())
    print(dim_tipo_consulta())
    carrega_tabelas()
