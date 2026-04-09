/* OBJETIVO: Validar a integridade e continuidade do Bloco K (Estoque), cruzando a abertura do período (K100) com os saldos físicos (K200).
  SE VAZIA: O Bloco K não foi enviado ou não há movimentação de estoque para os arquivos processados.
  SE CHEIA: Base de auditoria evolutiva que aponta onde o estoque "sumiu" entre um mês e outro ou onde o período foi aberto mas ficou sem dados.
  AÇÃO: Notificar o cliente sobre a omissão de inventário ou erro técnico na exportação do SPED para evitar multas por falta de escrituração do Bloco K.
*/


{{ config(materialized='view') }}

WITH periodos AS (
    SELECT DISTINCT 
        c.file_id,
        reg0.cnpj_empresa, -- <--- Agora vem do Registro 0000
        CAST(EXTRACT(YEAR FROM c.data_emissao) AS VARCHAR) as ano, 
        LPAD(CAST(EXTRACT(MONTH FROM c.data_emissao) AS VARCHAR), 2, '0') as mes
    FROM {{ ref('stg_c100') }} c
    INNER JOIN {{ ref('stg_0000') }} reg0 ON c.file_id = reg0.file_id -- <--- Join pelo arquivo
    WHERE c.data_emissao IS NOT NULL
),

k100_counts AS (
    SELECT file_id, COUNT(*) as qtd_k100 FROM {{ ref('stg_k100') }} GROUP BY 1
),

k200_counts AS (
    SELECT file_id, COUNT(*) as qtd_k200 FROM {{ ref('stg_k200') }} GROUP BY 1
),

consolidado AS (
    SELECT 
        p.cnpj_empresa,
        p.file_id,
        p.ano,
        p.mes,
        COALESCE(k1.qtd_k100, 0) as k100,
        COALESCE(k2.qtd_k200, 0) as k200
    FROM periodos p
    LEFT JOIN k100_counts k1 ON p.file_id = k1.file_id
    LEFT JOIN k200_counts k2 ON p.file_id = k2.file_id
),

comparativo AS (
    SELECT 
        *,
        LAG(k200) OVER (PARTITION BY cnpj_empresa ORDER BY ano, mes) as k200_anterior
    FROM consolidado
)

SELECT 
    cnpj_empresa,
    file_id,
    ano,
    mes,
    k100,
    k200,
    k200_anterior,
    CASE 
        WHEN k100 > 0 AND k200 = 0 THEN 'ERRO: Período K100 aberto sem itens no K200'
        WHEN k200 = 0 AND k200_anterior > 0 THEN 'ALERTA: Estoque sumiu'
        WHEN k100 = 0 AND k200 = 0 THEN 'Bloco K não informado'
        ELSE 'OK'
    END as status_auditoria
FROM comparativo
ORDER BY cnpj_empresa, ano, mes