select
    cast(file_id as varchar) as file_id,
    cast(qtd_lin_9999_2 as int) as total_linhas_declarado
from {{ source('gcp_sped', "9999.parquet") }}