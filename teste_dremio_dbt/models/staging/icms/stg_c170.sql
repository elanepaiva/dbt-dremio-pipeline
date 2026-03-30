select
    cast(file_id as varchar) as file_id,
    cast(father_line_n as int) as father_line_n, -- <--- Link com a nota pai
    cast(vl_icms_c170_15 as double) as vICMS_item_sped
from {{ source('gcp_sped', "C170.parquet") }}