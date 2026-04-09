{{ config(materialized='view') }}

select
    cast(file_id as varchar) as file_id,
    cast(line_n as integer) as line_n,
    cast(father_line_n as integer) as father_line_n,
    cast(num_item_c170_2 as varchar) as n_item,
    cast(cod_item_c170_3 as varchar) as id_produto,
    cast(cfop_c170_11 as varchar) as cfop,
    cast(cst_icms_c170_10 as varchar) as cst_icms,
    cast(vl_item_c170_7 as double) as valor_item,
    cast(qtd_c170_5 as varchar) as quantidade,
    cast(vl_bc_icms_c170_13 as double) as valor_base_icms,
    cast(vl_icms_c170_15 as double) as vl_icms_item_sped, 
    cast(vl_bc_icms_st_c170_16 as double) as valor_base_icms_st,
    cast(vl_icms_st_c170_18 as double) as valor_icms_st,
    cast(aliq_icms_c170_14 as double) as aliq_icms_item_sped
from {{ source('gcp_sped', "C170.parquet") }}