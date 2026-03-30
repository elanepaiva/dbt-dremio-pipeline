select
    cast(file_id as varchar) as file_id,
    cast(vl_tot_debitos_e110_2 as double) as total_debitos,
    cast(vl_tot_creditos_e110_6 as double) as total_creditos,
    cast(vl_icms_recolher_e110_13 as double) as icms_a_recolher,
    cast(vl_sld_apurado_e110_11 as double) as saldo_apurado
from {{ source('gcp_sped', 'E110.parquet') }}