with soma_itens as (
    select nfe_id, sum(vProd) as total_prod
    from {{ ref('stg_prod') }}
    group by 1
),
header as (
    select 
        nfe_id, 
        valor_total_nota,
        -- A conta oficial: Prod + IPI + Frete + Outros - Desconto
        (valor_total_nota - valor_ipi - valor_frete - valor_outras_despesas + valor_desconto) as total_prod_esperado
    from {{ ref('stg_icmstot') }} 
)
select 
    s.nfe_id, 
    s.total_prod, 
    h.total_prod_esperado
from soma_itens s
join header h on s.nfe_id = h.nfe_id
where abs(s.total_prod - h.total_prod_esperado) > 0.05