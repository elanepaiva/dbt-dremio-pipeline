/* OBJETIVO: Confrontar itens do XML contra itens do ICMS (C170) usando 7 chaves SPA.
   SE VAZIA: Não houve cruzamento entre as bases.
   SE CHEIA: Base consolidada para relatório de omissões e divergências de valores.
   AÇÃO: Utilizar no Alteryx para gerar os relatórios finais por CNPJ.
*/

{{ config(materialized='view') }}

WITH base_xml AS (
    SELECT 
        REGEXP_REPLACE(chave_acesso, '[^0-9]', '') as chave_limpa,
        CAST(CAST(n_item AS int) AS varchar) as n_item_str,
        vProd as vProd_xml,
        vICMS as vICMS_xml,
        quantidade as qtd_xml,
        codigo_produto as cod_prod_xml
    FROM {{ ref('fct_itens_notas_fiscais') }}
),

base_icms AS (
    SELECT 
        REGEXP_REPLACE(c100.chave_acesso, '[^0-9]', '') as chave_limpa_sped,
        CAST(CAST(c170.n_item AS int) AS varchar) as n_item_sped_str,
        c170.valor_item as vProd_sped,
        c170.vl_icms_item_sped as vICMS_sped,
        c170.quantidade as qtd_sped,
        c170.id_produto as cod_prod_sped,
        c170.cfop as cfop_sped
    FROM {{ ref('stg_c170') }} c170
    INNER JOIN {{ ref('stg_c100') }} c100 
        ON c170.file_id = c100.file_id 
        AND c170.father_line_n = c100.line_n
)

SELECT 
    COALESCE(x.chave_limpa, i.chave_limpa_sped) as chave_referencia,
    SUBSTRING(COALESCE(x.chave_limpa, i.chave_limpa_sped), 7, 14) as cnpj_emitente,
    SUBSTRING(COALESCE(x.chave_limpa, i.chave_limpa_sped), 3, 2) as ano_referencia,
    SUBSTRING(COALESCE(x.chave_limpa, i.chave_limpa_sped), 5, 2) as mes_referencia,
    
    COALESCE(x.n_item_str, i.n_item_sped_str) as item_referencia,
    i.cfop_sped,
    
    x.vProd_xml,
    i.vProd_sped,
    ROUND(COALESCE(x.vProd_xml,0) - COALESCE(i.vProd_sped,0), 2) as diff_vProd,
    
    x.vICMS_xml,
    i.vICMS_sped,
    ROUND(COALESCE(x.vICMS_xml,0) - COALESCE(i.vICMS_sped,0), 2) as diff_vICMS,

    -- Coluna 1: Nome Amigável (para o time de negócio)
    CASE 
        WHEN x.chave_limpa = i.chave_limpa_sped AND x.n_item_str = i.n_item_sped_str AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'Match 1: Chave+Item+Valor'
        WHEN x.chave_limpa = i.chave_limpa_sped AND ROUND(x.qtd_xml,3) = ROUND(CAST(i.qtd_sped AS double),3) AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'Match 2: Chave+Qtd+Valor'
        WHEN x.chave_limpa = i.chave_limpa_sped AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'Match 3: Chave+Valor'
        WHEN x.chave_limpa = i.chave_limpa_sped AND ROUND(x.qtd_xml,3) = ROUND(CAST(i.qtd_sped AS double),3) THEN 'Match 4: Chave+Qtd'
        WHEN x.chave_limpa = i.chave_limpa_sped AND x.cod_prod_xml = i.cod_prod_sped AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'Match 5: Chave+EAN+Valor'
        WHEN x.chave_limpa = i.chave_limpa_sped AND x.cod_prod_xml = i.cod_prod_sped THEN 'Match 6: Chave+EAN'
        WHEN x.chave_limpa = i.chave_limpa_sped THEN 'Match 7: Divergente (Apenas Chave)'
        WHEN x.chave_limpa IS NULL THEN 'Sobra SPED'
        WHEN i.chave_limpa_sped IS NULL THEN 'Omissão XML'
    END AS status_match,

    -- Coluna 2: Dicionário Alteryx (Para sua validação técnica)
    CASE 
        WHEN x.chave_limpa = i.chave_limpa_sped AND x.n_item_str = i.n_item_sped_str AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'SPA_CHAVE1 & SPA_CHAVE1'
        WHEN x.chave_limpa = i.chave_limpa_sped AND ROUND(x.qtd_xml,3) = ROUND(CAST(i.qtd_sped AS double),3) AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'SPA_CHAVE2 & SPA_CHAVE2'
        WHEN x.chave_limpa = i.chave_limpa_sped AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'SPA_CHAVE3 & SPA_CHAVE3'
        WHEN x.chave_limpa = i.chave_limpa_sped AND ROUND(x.qtd_xml,3) = ROUND(CAST(i.qtd_sped AS double),3) THEN 'SPA_CHAVE4 & SPA_CHAVE4'
        WHEN x.chave_limpa = i.chave_limpa_sped AND x.cod_prod_xml = i.cod_prod_sped AND ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) THEN 'SPA_CHAVE5 & SPA_CHAVE5'
        WHEN x.chave_limpa = i.chave_limpa_sped AND x.cod_prod_xml = i.cod_prod_sped THEN 'SPA_CHAVE6 & SPA_CHAVE6'
        WHEN x.chave_limpa = i.chave_limpa_sped THEN 'SPA_CHAVE9 & SPA_CHAVE3'
        WHEN x.chave_limpa IS NULL THEN 'Não Cruzou c/ XML - Visão SPED'
        WHEN i.chave_limpa_sped IS NULL THEN 'Não Cruzou c/ EFD ICMS/IPI - Visão XML'
    END AS dicionario_alteryx

FROM base_xml x
FULL OUTER JOIN base_icms i 
    ON x.chave_limpa = i.chave_limpa_sped 
    AND (x.n_item_str = i.n_item_sped_str OR ROUND(x.vProd_xml,2) = ROUND(i.vProd_sped,2) OR x.cod_prod_xml = i.cod_prod_sped)