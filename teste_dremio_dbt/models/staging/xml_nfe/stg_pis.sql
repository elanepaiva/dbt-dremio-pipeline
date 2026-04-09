select
    PIS_Id as nfe_id,
    PIS_nItem as n_item,
    cast(nullif(PIS_vBC, '') as double) as vBC_pis,
    cast(nullif(PIS_vPIS, '') as double) as vPIS
from {{ source('mysql_xml', 'pis') }}
inner join {{ ref('stg_log') }} l on PIS_Id = l.id