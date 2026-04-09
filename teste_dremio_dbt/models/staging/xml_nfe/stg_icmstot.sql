
select
    icmstot_Id as nfe_id,
    cast(icmstot_vNF as double) as valor_total_nota,
    cast(coalesce(icmstot_vFrete, 0) as double) as valor_frete,
    cast(coalesce(icmstot_vDesc, 0) as double) as valor_desconto,
    cast(coalesce(icmstot_vOutro, 0) as double) as valor_outras_despesas,
    cast(coalesce(icmstot_vIPI, 0) as double) as valor_ipi
from {{ source('mysql_xml', 'icmstot') }}