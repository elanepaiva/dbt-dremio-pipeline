with source as (
    select * from {{ source('mysql_xml', 'prod') }}
),
log as (
    -- Filtro de processados vindo da sua stg_log para evitar lixo e duplicatas
    select id from {{ ref('stg_log') }} 
)
select
    cast(p.prod_Id as varchar) as nfe_id, 
    cast(p.prod_nItem as int) as n_item,
    cast(p.prod_cProd as varchar) as codigo_produto,
    cast(p.prod_xProd as varchar) as descricao_produto,
    cast(p.prod_NCM as varchar) as ncm,
    cast(p.prod_CFOP as varchar) as cfop,
    cast(p.prod_uCom as varchar) as unidade_comercial,
    cast(nullif(p.prod_qCom, '') as double) as quantidade,
    cast(nullif(p.prod_vUnCom, '') as double) as valor_unitario,
    cast(nullif(p.prod_vProd, '') as double) as vProd,
    cast(p.prod_uTrib as varchar) as unidade_tributaria,
    cast(nullif(p.prod_qTrib, '') as double) as quantidade_tributaria
from source p
inner join log l on p.prod_Id = l.id