/* OBJETIVO: Agrupar os itens por nota fiscal para visão de capa (totalizadores).
   SE VAZIA: Itens não foram auditados ou base de origem está vazia.
   SE CHEIA: Visão resumida por Chave de Acesso para BI e Auditoria de Capa.
   AÇÃO: Validar o status geral da nota (Nota Válida vs Nota com Itens Erros).
*/


{{ config(materialized='view') }}

with base as (
    select * from {{ ref('fct_auditoria_qualidade_itens') }}
)

select 
    chave_acesso,
    numero_nota,
    data_emissao,
    nome_cliente,
    cnpj_cliente,
    uf_cliente,
    -- Somamos os itens para ter o total da nota nesta linha única
    sum(vProd) as valor_total_produtos,
    sum(vICMS) as valor_total_icms,
    -- Pegamos o "pior" status: se um item tiver erro, a nota toda aparece com atenção
    min(parecer_fiscal) as status_nota,
    min(is_valida) as nota_valida_flag
from base
group by 1, 2, 3, 4, 5, 6