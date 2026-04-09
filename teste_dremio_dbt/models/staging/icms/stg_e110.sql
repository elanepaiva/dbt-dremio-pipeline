select
    cast(file_id as varchar) as file_id,
    cast(vl_tot_debitos_e110_2 as double) as total_debitos,
    cast(vl_tot_creditos_e110_6 as double) as total_creditos,
    cast(vl_icms_recolher_e110_13 as double) as valor_a_recolher,
    cast(vl_sld_credor_transportar_e110_14 as double) as saldo_credor_proximo_mes
from {{ source('gcp_sped', "E110.parquet") }}