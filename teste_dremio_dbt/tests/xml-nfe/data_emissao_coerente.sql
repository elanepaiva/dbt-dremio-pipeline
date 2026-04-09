select
    nfe_id,
    data_emissao
from {{ ref('stg_ide') }}
where data_emissao > current_timestamp 
   or data_emissao < cast('2015-01-01' as timestamp)