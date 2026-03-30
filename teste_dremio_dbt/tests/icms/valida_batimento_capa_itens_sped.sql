with soma_itens as (
    select 
        file_id, 
        father_line_n, 
        sum(vICMS_item_sped) as total_icms_itens
    from {{ ref('stg_c170') }}
    group by 1, 2
),
capa as (
    select 
        file_id, 
        line_n, 
        chave_nfe,
        vICMS_sped as icms_capa
    from {{ ref('stg_c100') }}
)

select 
    c.chave_nfe,
    c.icms_capa,
    i.total_icms_itens,
    abs(c.icms_capa - i.total_icms_itens) as diferenca
from capa c
join soma_itens i 
    on c.file_id = i.file_id 
    and c.line_n = i.father_line_n -- O GRANDE AJUSTE!
where abs(c.icms_capa - i.total_icms_itens) > 0.01