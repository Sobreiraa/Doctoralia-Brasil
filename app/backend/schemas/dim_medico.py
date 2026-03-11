import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

schema_dim_medico = DataFrameSchema({
    "sk_medico": Column(pa.Int, nullable=False, checks=Check(lambda s: s > 0)),
    "sk_especializacao": Column(pa.Int, nullable=False, checks=Check(lambda s: s >= 0)),
    "id_medico": Column(pa.Int, nullable=False, checks=Check(lambda s: s > 0)),
    "nome": Column(pa.String, nullable=False),
    "titulo": Column(pa.String, nullable=True),
    "atende_remoto": Column(pa.Bool, nullable=True),
    "cidade": Column(pa.String, nullable=True),
    "estado": Column(pa.String, nullable=True),
})