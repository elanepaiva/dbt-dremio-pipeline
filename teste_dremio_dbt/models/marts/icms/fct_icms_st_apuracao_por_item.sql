/* OBJETIVO: Comparar o preço de venda atual com a base de cálculo da compra (ST).
  SE VAZIA: Não há vendas registradas ou os produtos não possuem ST.
  SE CHEIA: Mostra a Margem Real. Se Venda < Base Compra = Direito a Ressarcimento!
*/

{{ config(materialized='view') }}

with base_calculo as (
    select 
        file_id,
        id_produto,
        cfop,
        valor_item as v_venda,
        valor_base_icms_st as v_base_st_entrada 
    from {{ ref('stg_c170') }}
    where cfop like '5%' or cfop like '6%'
)

select 
    bc.*,
    round(bc.v_venda - bc.v_base_st_entrada, 2) as margem_real,
    -- O PULO DO GATO: Adicionamos o CAST para o Dremio não travar
    cast(
        case 
            when bc.v_venda < bc.v_base_st_entrada then 'RESSARCIMENTO (Venda < Base Compra)'
            when bc.v_venda > bc.v_base_st_entrada then 'COMPLEMENTO (Venda > Base Compra)'
            else '➖ EQUILÍBRIO'
        end 
    as varchar(100)) as analise_st
from base_calculo bc