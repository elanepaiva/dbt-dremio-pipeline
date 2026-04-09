/* OBJETIVO: Classificar a qualidade fiscal de cada item do XML.
   SE VAZIA: Não há itens processados na base fct_itens_notas_fiscais.
   SE CHEIA: Lista de itens com parecer fiscal (Tributação correta vs Erros óbvios).
   AÇÃO: Identificar notas que saíram do ERP com inconsistências de CST e Imposto.
*/


{{ config(materialized='view') }}

select 
    chave_acesso,
    data_emissao,
    numero_nota,
    nome_cliente,
    cnpj_cliente,
    uf_cliente,
    cfop,
    cst_icms,
    vProd,
    vICMS,
    -- Regra de Ouro: Classificação de cada linha
    case 
        when cfop IN ('5102', '6102', '5101', '6101') and cst_icms = '00' and vICMS = 0 
            then 'ERRO: TRIBUTADA SEM IMPOSTO'
        
        when vICMS > vProd 
            then 'ERRO: ICMS MAIOR QUE PRODUTO'
            
        when cst_icms in ('40', '41', '51') and vICMS = 0 
            then 'OK: ISENTO/DIFERIDO CONFORME CST'
            
        when cst_icms = '00' and vICMS > 0 
            then 'OK: TRIBUTADA COM SUCESSO'
            
        else 'OK: OUTRAS OPERACOES'
    end as parecer_fiscal,
    
    -- Coluna booleana para facilitar filtros no BI (0 ou 1)
    case 
        when (cfop IN ('5102', '6102') and cst_icms = '00' and vICMS = 0) then 0
        else 1 
    end as is_valida
from {{ ref('fct_itens_notas_fiscais') }}