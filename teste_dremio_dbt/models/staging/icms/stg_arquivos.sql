-- models/staging/icms/stg_arquivos.sql
select
    cast(file_id as varchar) as file_id,
    cast(cnpj as varchar) as cnpj_empresa,
    cast(file_name as varchar) as nome_arquivo,
    cast(initial_date as date) as data_inicio_apuracao,
    cast(final_date as date) as data_fim_apuracao,
    cast(retificadora as boolean) as is_retificadora,
    cast(transmissao_datetime as timestamp) as data_transmissao,
    cast(inscricao_estadual as varchar) as ie_empresa
from {{ source('gcp_sped', "arquivos.parquet") }}