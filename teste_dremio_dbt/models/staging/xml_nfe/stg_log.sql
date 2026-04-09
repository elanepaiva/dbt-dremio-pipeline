with source as (
    select * from {{ source('mysql_xml', 'log') }}
),
deduplicados as (
    select 
        ID,
        cast(Status as varchar) as status_processamento,
        Data as Data_processamento,
        row_number() over (partition by ID order by Data desc) as rn
    from source
    where cast(Status as varchar) = 'Processado' -- Filtro crítico aqui
)
select * from deduplicados where rn = 1