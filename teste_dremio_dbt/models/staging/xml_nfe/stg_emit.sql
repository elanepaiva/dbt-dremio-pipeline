select
    emit_Id as nfe_id,
    cast(emit_CNPJ as varchar) as cnpj_emitente,
    cast(emit_xNome as varchar) as nome_emitente
from {{ source('mysql_xml', 'emit') }}
inner join {{ ref('stg_log') }} l on emit_Id = l.id