{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_confronto_spa_itens') }}
WHERE status_match IN ('Match 7: Divergente (Apenas Chave)', 'Omissão XML')
  AND ABS(diff_vProd) > 0.01  -- Ignora diferenças de centavos (arredondamento)