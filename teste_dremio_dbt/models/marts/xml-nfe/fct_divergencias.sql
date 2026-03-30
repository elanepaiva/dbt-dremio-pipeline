select 
    prod_Id,
    nItem,
    vProd,
    vICMS,
    vBC_icms as bc_icms, -- Nome corrigido aqui
    -- Cálculo teórico (exemplo 18%)
    (vBC_icms * 0.18) as icms_esperado, 
    -- Diferença entre o banco e o cálculo
    (vICMS - (vBC_icms * 0.18)) as diferenca
from {{ ref('fct_sumarizacao_nfe') }}
where abs(vICMS - (vBC_icms * 0.18)) > 0.01