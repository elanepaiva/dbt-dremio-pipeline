select
    cast(file_id as varchar) as file_id,
    cast(line_n as int) as line_n,
    cast(chv_nfe_c100_9 as varchar) as chave_nfe,
    cast(vl_icms_c100_22 as double) as vICMS_sped,
    cast(vl_doc_c100_12 as double) as vProd_sped
from {{ source('gcp_sped', "C100.parquet") }}