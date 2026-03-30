with soma_itens as (
    select 
        file_id, 
        sum(vICMS_item_sped) as total_icms_itens
    from {{ ref('stg_c170') }}
    group by 1
),
capa as (
    select 
        file_id, 
        chave_nfe,
        vICMS_sped as icms_declarado_capa
    from {{ ref('stg_c100') }}
)

select 
    c.file_id,
    c.chave_nfe,
    c.icms_declarado_capa,
    i.total_icms_itens,
    (c.icms_declarado_capa - coalesce(i.total_icms_itens, 0)) as diferenca
from capa c
left join soma_itens i on c.file_id = i.file_id
where abs(c.icms_declarado_capa - coalesce(i.total_icms_itens, 0)) > 0.01