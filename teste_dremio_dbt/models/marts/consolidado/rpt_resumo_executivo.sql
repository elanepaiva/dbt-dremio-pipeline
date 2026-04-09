{{ config(materialized='view') }}

SELECT 
    SUBSTRING(chave_referencia, 7, 14) as cnpj_emitente, -- Extrai o CNPJ da Chave
    status_match,
    COUNT(*) as qtd_itens,
    SUM(vProd_xml) as total_xml,
    SUM(vProd_sped) as total_sped,
    SUM(diff_vProd) as total_divergencia_financeira
FROM {{ ref('fct_confronto_spa_itens') }}
GROUP BY 1, 2