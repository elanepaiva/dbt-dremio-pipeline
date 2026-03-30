with total_capa as (
    select 
        file_id, 
        sum(vICMS_sped) as icms_total_notas
    from {{ ref('stg_c100') }}
    group by 1
),
total_analitico as (
    select 
        file_id, 
        sum(vICMS_resumo) as icms_total_resumo
    from {{ ref('stg_c190') }}
    group by 1
)

select 
    c.file_id,
    c.icms_total_notas,
    a.icms_total_resumo,
    abs(c.icms_total_notas - a.icms_total_resumo) as diferenca
from total_capa c
join total_analitico a on c.file_id = a.file_id
where abs(c.icms_total_notas - a.icms_total_resumo) > 0.01