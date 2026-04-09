select
    nfe_id,
    n_item,
    vProd
from {{ ref('stg_prod') }}
where vProd < 0