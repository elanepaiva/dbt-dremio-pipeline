/* OBJETIVO: Identificar valores de ICMS-ST que podem ser recuperados ou creditados na entrada.
   SE VAZIA: Não foram identificadas oportunidades de crédito de ST nas compras/devoluções.
   SE CHEIA: Valores de imposto pago anteriormente que podem reduzir o débito atual (Ressarcimento).
   AÇÃO: Realizar a apropriação do crédito no livro fiscal ou CAT-42/SPED.
*/

{{ config(materialized='view') }}

select
    arq.cnpj_empresa,
    c170.id_produto,
    prod.nome_produto,
    c170.cfop,
    c170.cst_icms,
    sum(c170.valor_item) as total_compras,
    sum(c170.valor_base_icms_st) as total_base_st,
    sum(c170.valor_icms_st) as total_st_pago_na_entrada
from {{ ref('stg_c170') }} c170
left join {{ ref('stg_0200') }} prod on c170.file_id = prod.file_id and c170.id_produto = prod.id_produto
left join {{ ref('stg_arquivos') }} arq on c170.file_id = arq.file_id
where c170.cfop like '2%' -- Foco nas entradas que você já tem
  and c170.valor_icms_st > 0 -- Apenas o que gerou imposto retido
group by 1, 2, 3, 4, 5