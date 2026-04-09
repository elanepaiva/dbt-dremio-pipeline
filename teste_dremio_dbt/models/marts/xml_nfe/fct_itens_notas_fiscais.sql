/* OBJETIVO: Consolidar todos os dados granulares dos itens (prod, icms, pis, cofins) com dados da capa (ide, emit, dest).
   SE VAZIA: Falha grave no processamento das tabelas staging ou ausência de arquivos XML.
   SE CHEIA: Base principal para todos os cruzamentos e cálculos detalhados de auditoria.
   AÇÃO: Utilizar como origem para as views de qualidade, divergência e conciliação SPA.
*/

with prod as (select * from {{ ref('stg_prod') }}),
     ide as (select * from {{ ref('stg_ide') }}),
     emit as (select * from {{ ref('stg_emit') }}),
     dest as (select * from {{ ref('stg_dest') }}), -- 1. CTE do Cliente adicionada
     icms as (select * from {{ ref('stg_icms') }}),
     pis as (select * from {{ ref('stg_pis') }}),
     cofins as (select * from {{ ref('stg_cofins') }}),
     log as (select id, status_processamento from {{ ref('stg_log') }})

select 
    prod.nfe_id as chave_acesso,
    ide.data_emissao,
    ide.numero_nota,
    emit.cnpj_emitente,

    dest.cnpj_cliente,
    dest.nome_cliente,
    dest.uf_cliente,
    -----------------------------------------
    log.status_processamento, 
    prod.n_item,
    prod.codigo_produto,
    prod.descricao_produto,
    prod.ncm,
    prod.cfop,
    prod.quantidade,
    prod.valor_unitario,
    prod.vProd,
    icms.cst_icms,
    icms.origem_produto,
    icms.vICMS,
    icms.vBC as vBC_icms,
    pis.vPIS,
    pis.vBC_pis,
    cofins.vCOFINS,
    cofins.vBC_cofins
from prod
inner join ide on prod.nfe_id = ide.nfe_id
inner join log on prod.nfe_id = log.id 
left join emit on prod.nfe_id = emit.nfe_id
left join dest on prod.nfe_id = dest.nfe_id -- Join do Cliente adicionado
left join icms on prod.nfe_id = icms.nfe_id and prod.n_item = icms.n_item
left join pis on prod.nfe_id = pis.nfe_id and prod.n_item = pis.n_item
left join cofins on prod.nfe_id = cofins.nfe_id and prod.n_item = cofins.n_item