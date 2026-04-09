-- models/staging/xml-nfe/stg_ide.sql
select
    ide_Id as nfe_id,
    cast(ide_dhEmi as timestamp) as data_emissao,
    cast(ide_tpNF as int) as tipo_operacao,
    cast(ide_nNF as int) as numero_nota
from {{ source('mysql_xml', 'ide') }}
inner join {{ ref('stg_log') }} l on ide_Id = l.id