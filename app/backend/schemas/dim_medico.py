import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

schema_dim_medico = DataFrameSchema({
    "id": Column(
        pa.Int,
        nullable=False,
        checks=Check(lambda s: s > 0)  # só valores positivos
    ),
    "nome": Column(pa.String, nullable=False),
    "titulo": Column(pa.String, nullable=True),
    "atende_remoto": Column(pa.Bool, nullable=True),
    "cidade": Column(pa.String, nullable=True),
    "estado": Column(pa.String, nullable=True),
    "qtd_consultas_avaliadas": Column(
        pa.Int,  # precisa ser numérico para checar positivo
        nullable=True,
        checks=Check(lambda s: s > 0)  # só positivos
    )
})