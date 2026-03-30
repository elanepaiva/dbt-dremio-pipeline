select
    cast(file_id as varchar) as file_id,
    cast(cnpj_0000_7 as varchar) as cnpj,
    cast(ie_0000_10 as varchar) as inscricao_estadual,
    cast(dt_ini_0000_4 as date) as data_inicio,
    cast(dt_fin_0000_5 as date) as data_fim
from {{ source('gcp_sped', '0000.parquet') }}