select
    cast(file_id as varchar) as file_id,
    cast(cod_part_0150_2 as varchar) as id_participante,
    cast(nome_0150_3 as varchar) as nome_participante,
    cast(cnpj_0150_5 as varchar) as cnpj_participante,
    cast(cpf_0150_6 as varchar) as cpf_participante,
    cast(ie_0150_7 as varchar) as ie_participante
from {{ source('gcp_sped', "0150.parquet") }}