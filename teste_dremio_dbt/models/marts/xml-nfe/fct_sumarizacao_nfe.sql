with cte_prod as (
    select * from {{ ref('stg_prod') }}
),
cte_icms as (
    select * from {{ ref('stg_icms') }}
),
cte_cofins as (
    select * from {{ ref('stg_cofins') }}
),
cte_ipi as (
    select * from {{ ref('stg_ipi') }}
),
cte_log_xml as (
    select * from {{ ref('stg_log') }}
)

select 
    p.chave_nfe,
    p.prod_Id,
    p.nItem,
    p.vProd,
    i.vICMS,
    i.vBC as vBC_icms,
    c.vBC_cofins,
    c.vCOFINS,
    ip.vIPI,
    l.status_processamento
from cte_prod p -- <--- Agora apontando para a CTE renomeada
left join cte_icms i 
    on p.prod_Id = i.ICMS_Id and p.nItem = i.ICMS_nItem 
left join cte_cofins c 
    on p.prod_Id = c.id and p.nItem = c.item -- <-- USANDO OS ALIASES DO STAGING
left join cte_ipi ip 
    on p.prod_Id = ip.id and p.nItem = ip.item -- <-- CHEQUE SE O STG_
left join cte_log_xml l 
    on p.prod_Id = l.id