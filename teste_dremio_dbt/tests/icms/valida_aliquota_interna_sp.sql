/*
  AUDITORIA: VALIDAÇÃO DE ALÍQUOTA INTERNA (SP)
  OBJETIVO: Identificar itens de saída dentro do estado (CFOP 5xxx) 
  que foram tributados com alíquota diferente de 18%.
  REGRA: Se o CST é 00 (Tributada integralmente) e o destino é interno, 
  a alíquota deve ser obrigatoriamente 18%.
*/

select
    file_id,
    father_line_n as linha_nfe,
    id_produto,
    cfop,
    aliq_icms_item_sped as aliquota_declarada
from {{ ref('stg_c170') }}
where cfop like '5%'                          -- Saídas Internas
  and cst_icms = '00'                         -- Tributadas Integralmente
  and aliq_icms_item_sped != 18.0             -- Diferente de 18%
  and aliq_icms_item_sped > 0                 -- Ignora casos de erro que seriam isentos