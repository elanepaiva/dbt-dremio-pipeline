select
    ICMS_Id,
    ICMS_nItem,
    cast(nullif(ICMS_vICMS, '') as double) as vICMS,
    cast(nullif(ICMS_vBC, '') as double) as vBC
from {{ source('mysql_xml', 'icms') }}