-- O dbt reporta falha se esta query retornar qualquer linha.
-- Aqui limpamos o 'NFe' apenas para contar os 44 números obrigatórios.
select
    nfe_id,
    length(replace(cast(nfe_id as varchar), 'NFe', '')) as tamanho_apenas_numeros
from {{ ref('stg_ide') }}
where length(replace(cast(nfe_id as varchar), 'NFe', '')) <> 44