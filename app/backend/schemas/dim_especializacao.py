import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

schema_especializacao = DataFrameSchema({
    "sk_especializacao": Column(pa.Int, nullable=False, checks=Check(lambda s: s >= 0)),
    "id": Column(pa.Int, nullable=False, checks=Check(lambda s: s > 0)),
    "especializacao": Column(pa.String, Check.str_length(min_value=1), nullable=True)
})