{{ config(materialized='view') }}

SELECT 
    chave_referencia,
    item_referencia,
    vProd_sped,
    vICMS_sped,
    -- Extrai o Ano/Mês da chave para você filtrar no dashboard
    SUBSTRING(chave_referencia, 3, 2) as ano_emissao,
    SUBSTRING(chave_referencia, 5, 2) as mes_emissao
FROM {{ ref('fct_confronto_spa_itens') }}
WHERE status_match = 'Sobra SPED'