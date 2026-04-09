{{ config(materialized='view') }}

SELECT 
    reg_k100_1 as registro,
    dt_ini_k100_2 as data_inicio_periodo,
    dt_fin_k100_3 as data_fim_periodo,
    file_id,
    line_n,
    father_line_n
FROM {{ source('gcp_sped', 'K100.parquet') }}