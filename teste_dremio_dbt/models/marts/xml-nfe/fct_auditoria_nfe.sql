with base as (
    select * from {{ ref('fct_sumarizacao_nfe') }}
),
totais as (
    select
        count(distinct prod_Id) as total_xmls,
        sum(vProd) as total_produtos_banco,
        sum(vICMS) as total_icms_banco,
        sum(vBC_icms) as total_bc_icms_banco,
        sum(vCOFINS) as total_cofins_banco
    from base
)
select 
    total_xmls,
    total_produtos_banco,
    total_icms_banco,
    total_bc_icms_banco,
    total_cofins_banco,
    -- Usando texto simples para evitar erro de encoding no Dremio
    case 
        when total_icms_banco > total_bc_icms_banco then 'ERRO: ICMS MAIOR QUE BC'
        else 'SUCESSO: VALORES OK'
    end as status_auditoria
from totais