with base as (
    select * from {{ ref('fct_sumarizacao_nfe') }}
)
select 
    prod_Id,
    vProd,
    vICMS,
    -- Regra de alerta de erro
    case 
        when vICMS = 0 and status_processamento = 'OK' then 'ERRO: Sem ICMS em nota processada'
        when vProd > 10000 then 'ALERTA: Nota de alto valor'
        else 'OK'
    end as status_auditoria
from base