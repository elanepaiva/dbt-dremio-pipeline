{{ config(materialized='view') }}

SELECT 
    reg_k200_1 as registro,
    dt_est_k200_2 as data_estoque,
    cod_item_k200_3 as codigo_item,
    CAST(qtd_k200_4 AS double) as quantidade,
    ind_est_k200_5 as indicador_propriedade,
    cod_part_k200_6 as codigo_participante,
    file_id,
    line_n,
    father_line_n
FROM {{ source('gcp_sped', 'K200.parquet') }}