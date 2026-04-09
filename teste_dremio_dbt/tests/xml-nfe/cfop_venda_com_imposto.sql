select
    chave_acesso,
    nome_cliente,
    cfop,
    cst_icms,
    vProd,
    vICMS
from {{ ref('fct_itens_notas_fiscais') }}
where cfop IN ('5102', '6102', '5101', '6101')
  -- Filtramos para pegar apenas erros reais:
  and cst_icms = '00' -- Tributada Integralmente (Exige imposto)
  and vICMS = 0        -- Mas o valor está zerado