select
    nfe_id,
    n_item,
    cfop
from {{ ref('stg_prod') }}
where 
    substring(cast(cfop as varchar), 1, 1) NOT IN ('5', '6', '7')