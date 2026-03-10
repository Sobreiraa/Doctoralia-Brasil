import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

schema_especializacao = DataFrameSchema({
    "especializacao": Column(pa.String, Check.str_length(min_value=1), nullable=True)
})