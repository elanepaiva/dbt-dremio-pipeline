/* OBJETIVO: Garantir que todos os arquivos XML recebidos no banco foram processados.
   SE VAZIA: Falha crítica na leitura dos logs ou das tabelas de negócio.
   SE CHEIA: Mostra o gap entre arquivos brutos e notas transformadas.
   AÇÃO: Investigar XMLs que estão no Log mas não chegaram no consolidado (Erros de Schema).
*/



{{ config(materialized='view') }}

with logs as (
    -- O que entrou no sistema (Total de arquivos XML detectados)
    select count(distinct id) as qtd_xmls_recebidos from {{ ref('stg_log') }}
),
notas_finais as (
    -- O que chegou na sua tabela consolidada (As 2.438 notas)
    select count(distinct chave_acesso) as qtd_notas_processadas from {{ ref('fct_notas_fiscais_consolidado') }}
)

select 
    l.qtd_xmls_recebidos,
    n.qtd_notas_processadas,
    (l.qtd_xmls_recebidos - n.qtd_notas_processadas) as notas_nao_processadas,
    case 
        when l.qtd_xmls_recebidos = n.qtd_notas_processadas then 'INTEGRIDADE 100%: TUDO OK'
        when (l.qtd_xmls_recebidos - n.qtd_notas_processadas) > 0 then 'ALERTA: EXISTEM XMLS RETIDOS NO LOG'
        else 'CHECK: VOLUME DIVERGENTE'
    end as status_conferencia
from logs l, notas_finais n