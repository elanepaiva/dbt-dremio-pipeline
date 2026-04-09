
--O valor de ICMS da capa da nota (C100) tem que ser igual à soma do analítico (C190). 
--Se não bater, o SPED está inválido para o FISCO

with capa as (
    select file_id, line_n, valor_icms_sped as v_capa
    from {{ ref('stg_c100') }}
),
analitico as (
    select file_id, father_line_n, sum(valor_icms) as v_analitico
    from {{ ref('stg_c190') }}
    group by 1, 2
)
select 
    c.file_id, c.line_n, c.v_capa, a.v_analitico
from capa c
join analitico a on c.file_id = a.file_id and c.line_n = a.father_line_n
where abs(c.v_capa - a.v_analitico) > 0.05 -- Tolerância de 5 centavos