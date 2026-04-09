with icms_source as (
    select 
        ICMS_Id as nfe_id,
        ICMS_nItem as n_item,
        icms_CST as cst_icms, 
        icms_orig as origem_produto,
        cast(nullif(ICMS_vICMS, '') as double) as vICMS,
        cast(nullif(ICMS_vBC, '') as double) as vBC
    from {{ source('mysql_xml', 'icms') }}
),
log as (
    select id from {{ ref('stg_log') }} -- Já filtrado por 'Processado' e sem duplicatas
)
select i.* from icms_source i
inner join log l on i.nfe_id = l.id

