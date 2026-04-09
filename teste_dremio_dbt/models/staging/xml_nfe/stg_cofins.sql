select
    COFINS_Id as nfe_id,
    COFINS_nItem as n_item,
    cast(nullif(COFINS_vBC, '') as double) as vBC_cofins,
    cast(nullif(COFINS_vCOFINS, '') as double) as vCOFINS
from {{ source('mysql_xml', 'cofins') }}