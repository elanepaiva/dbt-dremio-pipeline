select
    id,
    cast(Status as varchar) as status_processamento, -- <--- Adicione o "as" aqui
    Data as Data_processamento
from {{ source('mysql_xml', 'log') }}