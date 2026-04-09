/* OBJETIVO: Validar se o total da capa do SPED bate com a soma dos itens (analítico).
   SE VAZIA: Arquivo SPED consistente entre totais e detalhes.
   SE CHEIA: Erro de integridade no arquivo; o SPED será rejeitado pelo fisco.
   AÇÃO: Corrigir a geração do arquivo magnético ou ajuste manual no PVA.
*/

{{ config(materialized='view') }}

with capa as (
    select 
        file_id, 
        line_n, 
        chave_acesso, 
        numero_nota, 
        valor_icms_sped as v_capa
    from {{ ref('stg_c100') }}
),
analitico as (
    select 
        file_id, 
        father_line_n, 
        sum(valor_icms) as v_analitico
    from {{ ref('stg_c190') }}
    group by 1, 2
)

select 
    c.chave_acesso,
    c.numero_nota,
    c.v_capa as valor_icms_cabecalho,
    a.v_analitico as valor_icms_resumo_analitico,
    round(c.v_capa - a.v_analitico, 2) as diferenca_centavos,
    -- O PULO DO GATO: Adicionamos o CAST para evitar o erro de álgebra do Dremio
    cast(
        case 
            when abs(c.v_capa - a.v_analitico) > 0.05 then '🚨 DIVERGÊNCIA'
            else 'OK'
        end 
    as varchar(50)) as status_auditoria
from capa c
left join analitico a 
    on c.file_id = a.file_id 
    and c.line_n = a.father_line_n
where abs(c.v_capa - a.v_analitico) > 0.05