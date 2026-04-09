/* OBJETIVO: Identificar operações de saída que geram direito ao ressarcimento do ICMS-ST (ex: vendas para fora do estado ou isentas).
   SE VAZIA: Não houve saídas com direito a crédito/ressarcimento no período analisado.
   SE CHEIA: Notas fiscais que permitem recuperar o imposto retido anteriormente na cadeia.
   AÇÃO: Gerar o arquivo para CAT-42/ADRC-ST ou lançar o crédito na apuração conforme regulamento estadual.
*/

{{ config(materialized='view') }}

select
    arq.cnpj_empresa,
    c100.data_emissao,
    c100.chave_acesso,
    c170.id_produto,
    prod.nome_produto,
    c170.cfop,
    c170.cst_icms,
    c170.valor_item,
    -- Campos específicos de ST retido anteriormente (muito comum no registro C170 ou C190)
    c170.valor_base_icms_st as vBC_ST_retido,
    c170.valor_icms_st as vICMS_ST_retido
from {{ ref('stg_c170') }} c170
inner join {{ ref('stg_c100') }} c100 on c170.file_id = c100.file_id and c170.father_line_n = c100.line_n
left join {{ ref('stg_0200') }} prod on c170.file_id = prod.file_id and c170.id_produto = prod.id_produto
left join {{ ref('stg_arquivos') }} arq on c170.file_id = arq.file_id
where c170.cst_icms in ('60', '10', '70') -- Códigos de Substituição Tributária
  and c170.cfop like '6%' -- Foco em vendas interestaduais para ressarcimento