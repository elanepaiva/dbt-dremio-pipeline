/* OBJETIVO: Cruzar o ST pago na compra com o ST debitado/ressarcido na venda.
   SE VAZIA: Ausência de movimentação de entrada ou saída para o período.
   SE CHEIA: Descompasso de valores que impacta diretamente o fluxo de caixa do imposto.
   AÇÃO: Validar a conta corrente de ST e identificar perdas de crédito.
*/


{{ config(materialized='view') }}

with entradas as (
    -- Pegamos as compras (CFOP inicia com 1 ou 2) que tiveram retenção de ST
    select 
        file_id,
        id_produto,
        sum(valor_item) as valor_total_compra,
        sum(valor_base_icms_st) as base_st_entrada,
        sum(valor_icms_st) as icms_st_pago_na_entrada
    from {{ ref('stg_c170') }}
    where cfop like '1%' or cfop like '2%'
    group by 1, 2
),

saidas as (
    -- Pegamos as vendas (CFOP inicia com 5 ou 6)
    select 
        file_id,
        id_produto,
        cfop,
        cst_icms,
        sum(valor_item) as valor_total_venda,
        sum(valor_base_icms_st) as base_st_saida_declarada
    from {{ ref('stg_c170') }}
    where cfop like '5%' or cfop like '6%'
    group by 1, 2, 3, 4
)

select 
    s.id_produto,
    p.nome_produto,
    s.cfop,
    s.cst_icms,
    s.valor_total_venda,
    e.icms_st_pago_na_entrada as credito_st_disponivel,
    -- Regra de Ouro: Se vendeu pra fora (CFOP 6) o ST pago na entrada vira Ressarcimento
    case 
        when s.cfop like '6%' then 'POTENCIAL RESSARCIMENTO'
        when s.cst_icms = '60' then 'ST RETIDO ANTERIORMENTE'
        else 'OPERAÇÃO NORMAL'
    end as status_projeto
from saidas s
left join entradas e on s.file_id = e.file_id and s.id_produto = e.id_produto
left join {{ ref('stg_0200') }} p on s.file_id = p.file_id and s.id_produto = p.id_produto