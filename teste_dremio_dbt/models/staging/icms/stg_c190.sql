select
    cast(file_id as varchar) as file_id,
    cast(reg_id as varchar) as reg_id,
    cast(cst_icms_c190_2 as varchar) as cst_icms,
    cast(cfop_c190_3 as varchar) as cfop,
    cast(aliq_icms_c190_4 as double) as aliq_icms,
    cast(vl_opr_c190_5 as double) as vOperacao_resumo,
    cast(vl_bc_icms_c190_6 as double) as vBC_icms_resumo,
    cast(vl_icms_c190_7 as double) as vICMS_resumo,
    cast(vl_bc_icms_st_c190_8 as double) as vBC_st_resumo,
    cast(vl_icms_st_c190_9 as double) as vICMS_st_resumo
from {{ source('gcp_sped', 'C190.parquet') }}