/* OBJETIVO: Validar se a alíquota interna aplicada segue a legislação de SP.
   SE VAZIA: Todas as alíquotas internas aplicadas estão de acordo com o esperado.
   SE CHEIA: Erro na parametrização de impostos (ex: 18% onde deveria ser 12% ou 25%).
   AÇÃO: Ajustar a matriz tributária no ERP para evitar pagamentos indevidos.
*/

select
    c100.chave_acesso,
    c170.id_produto,
    c170.cfop,
    c170.aliq_icms_item_sped
from {{ ref('stg_c170') }} as c170
inner join {{ ref('stg_c100') }} as c100 
    on c170.file_id = c100.file_id 
    and c170.father_line_n = c100.line_n
where c170.cfop like '5%'                     -- Saídas Internas
  and c170.cst_icms = '00'                    -- Tributadas integralmente
  and c170.aliq_icms_item_sped not in (0, 18) -- Diferente do padrão SP (18%) ou isento (0)