/* OBJETIVO: Detalhamento unitário (por item/SKU) de toda a tributação de ICMS das notas.
   SE VAZIA: Erro na leitura dos itens das notas fiscais (C170/registros analíticos).
   SE CHEIA: Base de dados granular para auditoria de alíquotas, reduções de base e CSTs.
   AÇÃO: Cruzar com o cadastro de produtos para identificar erros de parametrização por NCM.
*/

{{ config(materialized='view') }}

with 
capa as ( select * from {{ ref('stg_c100') }} ),
itens as ( select * from {{ ref('stg_c170') }} ),
produtos as ( select * from {{ ref('stg_0200') }} ),
arquivos as ( select * from {{ ref('stg_arquivos') }} )

select
    arq.cnpj_empresa,
    arq.data_inicio_apuracao,
    c.chave_acesso,
    c.numero_nota,
    i.n_item,
    i.id_produto,
    p.nome_produto,
    p.ncm,
    i.cfop,
    i.cst_icms,
    
    -- Valores de Mercadoria e ICMS Próprio
    i.valor_item, 
    i.valor_base_icms,
    i.aliq_icms_item_sped,

    -- Valores de ICMS ST (Campos 16 e 18 do C170)
    i.valor_base_icms_st,
    i.valor_icms_st,

    -- Auditoria de ICMS Próprio: Cálculo esperado vs Informado
    cast(
        case 
            when abs(i.valor_item - round(i.valor_base_icms * (i.aliq_icms_item_sped / 100), 2)) > 0.01 
            then 'ERRO DE CÁLCULO'
            else 'OK'
        end 
    as varchar(20)) as status_auditoria_item

from itens i
inner join capa c on i.file_id = c.file_id and i.father_line_n = c.line_n
left join produtos p on i.file_id = p.file_id and i.id_produto = p.id_produto
left join arquivos arq on i.file_id = arq.file_id