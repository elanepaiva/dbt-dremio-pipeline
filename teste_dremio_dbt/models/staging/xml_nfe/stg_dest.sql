-- models/staging/xml-nfe/stg_dest.sql
with destinatario as (
    select 
        dest_Id as nfe_id,
        dest_CNPJ as cnpj_cliente,
        dest_xNome as nome_cliente
    from {{ source('mysql_xml', 'dest') }}
),
endereco as (
    select 
        enderDest_Id as nfe_id,
        enderDest_UF as uf_cliente
    from {{ source('mysql_xml', 'enderDest') }} -- Nome da tabela no seu MySQL
)

select
    d.nfe_id,
    d.cnpj_cliente,
    d.nome_cliente,
    e.uf_cliente
from destinatario d
left join endereco e on d.nfe_id = e.nfe_id