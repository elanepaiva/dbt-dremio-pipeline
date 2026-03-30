select
    COFINS_Id as id,
    COFINS_nItem as item,
    cast(nullif(COFINS_vBC, '') as double) as vBC_cofins,
    cast(nullif(COFINS_vCOFINS, '') as double) as vCOFINS
from {{ source('mysql_xml', 'cofins') }}