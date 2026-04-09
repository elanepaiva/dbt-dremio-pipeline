select
    cast(file_id as varchar) as file_id,
    cast(cod_item_0200_2 as varchar) as id_produto,
    cast(descr_item_0200_3 as varchar) as nome_produto,
    cast(cod_ncm_0200_8 as varchar) as ncm,
    cast(unid_inv_0200_6 as varchar) as unidade_medida,
    -- ADICIONE ESTA LINHA:
    cast(cest_0200_13 as varchar) as cod_cest
from {{ source('gcp_sped', "0200.parquet") }}