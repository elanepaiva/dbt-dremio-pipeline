/* OBJETIVO: Analisar vendas para outros estados com retenção de ST.
   SE VAZIA: Não houve operações interestaduais sujeitas a ST no período.
   SE CHEIA: Base para conferência de GNREs e guias pagas a outros estados.
   AÇÃO: Conferir se o imposto foi retido e recolhido corretamente para a UF de destino.
*/

{{ config(materialized='view') }}

select 
    arq.data_inicio_apuracao as mes_referencia,
    c170.id_produto,
    prod.nome_produto,
    c170.cfop,
    count(*) as qtd_notas,
    sum(c170.valor_item) as valor_venda_total,
    -- Estimativa de crédito baseada em 18% (ajuste conforme o estado)
    sum(round(c170.valor_item * 0.18, 2)) as estimativa_credito_recuperavel
from {{ ref('stg_c170') }} c170
left join {{ ref('stg_0200') }} prod on c170.file_id = prod.file_id and c170.id_produto = prod.id_produto
left join {{ ref('stg_arquivos') }} arq on c170.file_id = arq.file_id
where c170.cfop like '6%' -- Apenas interestadual
  and c170.cst_icms in ('10', '60', '70') -- Apenas com ST prévio
group by 1, 2, 3, 4