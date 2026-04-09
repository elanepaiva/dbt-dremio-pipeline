select
    cast(file_id as varchar) as file_id,
    cast(father_line_n as int) as father_line_n,
    cast(cst_icms_c190_2 as varchar) as cst_icms,
    cast(cfop_c190_3 as varchar) as cfop,
    cast(aliq_icms_c190_4 as double) as aliq_icms,
    cast(vl_opr_c190_5 as double) as valor_operacao,
    cast(vl_bc_icms_c190_6 as double) as valor_base_icms,
    cast(vl_icms_c190_7 as double) as valor_icms
from {{ source('gcp_sped', "C190.parquet") }}
