/* OBJETIVO: Auditar o cálculo de Substituição Tributária nas notas de entrada.
   SE VAZIA: Não foram processadas notas de entrada com incidência de ST.
   SE CHEIA: Divergências entre o imposto destacado e o cálculo esperado (ICMS-ST).
   AÇÃO: Revisar lançamentos fiscais ou notas de fornecedores.
*/

{{ config(materialized='view') }}

select 
    id_produto,
    nome_produto,
    cfop,
    valor_item,
    valor_base_icms_st,
    valor_icms_st,
    case 
        when valor_icms_st = 0 and cfop like '2%' then 'COMPRA SEM ST RETIDO'
        when valor_icms_st > 0 then 'ST RECOLHIDO NA ENTRADA'
        else 'Verificar'
    end as status_st_entrada
from {{ ref('fct_icms_itens_detalhado') }}
where cfop like '2%'