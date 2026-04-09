-- tests/icms/teste_venda_com_icms_sped.sql
select
    file_id,
    father_line_n, -- Linha da nota pai no arquivo
    n_item,
    cfop,
    cst_icms,
    valor_icms_st
from {{ ref('stg_c170') }}
where 
    -- Filtra apenas operações que DEVERIAM ter ICMS (Vendas normais)
    cfop IN ('5101', '5102', '6101', '6102') 
    and cst_icms = '00' -- Tributada Integralmente
    and valor_icms_st = 0  -- Se o imposto estiver zerado, o teste captura o erro