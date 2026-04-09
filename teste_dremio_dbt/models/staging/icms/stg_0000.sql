select
    cast(file_id as varchar) as file_id,
    cast(line_n as int) as line_n,
    cast(nome_0000_6 as varchar) as nome_empresa,
    cast(cnpj_0000_7 as varchar) as cnpj_empresa,
    cast(uf_0000_9 as varchar) as uf_empresa,
    cast(ie_0000_10 as varchar) as ie_empresa,
    cast(dt_ini_0000_4 as date) as data_inicio,
    cast(dt_fin_0000_5 as date) as data_fim
from {{ source('gcp_sped', "0000.parquet") }}