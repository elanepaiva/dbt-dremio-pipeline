-- O dbt reporta ERRO se encontrar alguma linha aqui.
-- Buscamos CFOPs que NÃO começam com 5, 6 ou 7 (Saídas).
select
    prod_Id,
    prod_nItem,
    prod_CFOP
from {{ ref('stg_prod') }}
where 
    substring(cast(prod_CFOP as varchar), 1, 1) NOT IN ('5', '6', '7')