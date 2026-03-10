from backend.pipeline.extrair import extrai_csv
from backend.pipeline.transformar import transformando_df, dim_medico, dim_data, dim_especializacao, dim_tipo_consulta
import pandas as pd

df = extrai_csv("data/input")
transformando_df(df)

dim_medico(df)


