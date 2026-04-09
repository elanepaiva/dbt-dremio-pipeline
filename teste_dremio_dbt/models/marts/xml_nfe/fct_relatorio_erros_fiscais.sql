/* OBJETIVO: Listar notas com erros graves de tributação (CST 00 sem imposto destacado).
   SE VAZIA: Não foram detectados erros de CST vs Valor de ICMS.
   SE CHEIA: Notas que foram emitidas incorretamente e podem gerar autuação por falta de débito.
   AÇÃO: Reportar ao faturamento/comercial para correção de processos ou emissão de nota complementar.
*/


{{ config(materialized='view') }}

select 
    chave_acesso,
    numero_nota,
    nome_cliente,
    cfop,
    cst_icms,
    vProd,
    vICMS,
    'ERRO: CST 00 COM ICMS ZERADO' as motivo_divergencia
from {{ ref('fct_itens_notas_fiscais') }}
where cfop IN ('5102', '6102', '5101', '6101')
  and cst_icms = '00' 
  and vICMS = 0