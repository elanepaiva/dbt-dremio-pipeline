-- O dbt considera que o teste FALHOU se esta query retornar qualquer linha.
-- Portanto, buscamos valores menores que zero.
select
    total_produtos_banco
from {{ ref('fct_auditoria_nfe') }}
where total_produtos_banco < 0