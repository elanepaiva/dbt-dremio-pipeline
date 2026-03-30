with soma_itens as (
    select 
        file_id, 
        father_line_n, 
        sum(vICMS_item_sped) as total_icms_itens
    from {{ ref('stg_c170') }}
    group by 1, 2
),
analitico as (
    select 
        file_id, 
        sum(vICMS_resumo) as total_icms_analitico
    -- Se você criou o stg_c190 com line_n, use aqui também. 
    -- Se não, vamos bater pelo file_id para simplificar o diagnóstico.
    from {{ ref('stg_c190') }}
    group by 1
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
    c.icms_capa as valor_na_capa,
    i.total_icms_itens as valor_nos_itens,
    (c.icms_capa - i.total_icms_itens) as diferenca_itens,
    c.file_id
from capa c
left join soma_itens i 
    on c.file_id = i.file_id 
    and c.line_n = i.father_line_n
where abs(c.icms_capa - coalesce(i.total_icms_itens, 0)) > 0.01