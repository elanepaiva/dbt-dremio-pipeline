/* OBJETIVO: Validar se os produtos possuem o código CEST (obrigatório para ST).
  SE VAZIA: Não há cadastro de produtos (0200) processado.
  SE CHEIA: Itens com 'SEM CEST' impedem o pedido de ressarcimento ou geram multas.
  AÇÃO: Exportar para o cliente corrigir o cadastro no ERP.
*/

{{ config(materialized='view') }}

select 
    arq.cnpj_empresa,
    p.id_produto,
    p.nome_produto,
    p.ncm,
    p.cod_cest,
    case 
        when p.cod_cest is null or p.cod_cest = '' then 'SEM CEST (IMPEDIMENTO)'
        when length(p.cod_cest) < 7 then 'CEST INVÁLIDO'
        else 'OK'
    end as status_cadastro
from {{ ref('stg_0200') }} p
left join {{ ref('stg_arquivos') }} arq on p.file_id = arq.file_id
-- Filtramos apenas NCMs que geralmente têm ST (ex: começam com 33, 38, 40, 87...)
-- Você pode ajustar essa lista conforme o setor do seu cliente
where p.ncm like '33%' or p.ncm like '40%' or p.ncm like '87%'