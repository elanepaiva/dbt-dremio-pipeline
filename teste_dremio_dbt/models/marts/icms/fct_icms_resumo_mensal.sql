/* OBJETIVO: Consolidar os totais de ICMS (Próprio e ST) para apuração mensal e fechamento.
   SE VAZIA: Falha no processamento ou ausência total de movimento fiscal no mês.
   SE CHEIA: Visão executiva dos débitos, créditos e saldo a pagar/credor.
   AÇÃO: Utilizar como base para o preenchimento da GIA e conferência do SPED Fiscal.
*/


{{ config(materialized='view') }}

select
    arq.cnpj_empresa,
    arq.data_inicio_apuracao as competencia,
    c190.cfop,
    -- O NOME DEVE SER aliq_icms (conforme definido na stg_c190)
    c190.aliq_icms, 
    sum(c190.valor_operacao) as total_operacao,
    sum(c190.valor_base_icms) as total_base_icms,
    sum(c190.valor_icms) as total_icms
from {{ ref('stg_c190') }} c190
left join {{ ref('stg_arquivos') }} arq on c190.file_id = arq.file_id
group by 1, 2, 3, 4