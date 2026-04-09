/* OBJETIVO: Identificar itens onde o cálculo do ICMS/PIS difere da alíquota padrão (18% / 1.65%).
   SE VAZIA: Todos os cálculos estão dentro da margem de tolerância.
   SE CHEIA: Itens com destaque de imposto incorreto no XML.
   AÇÃO: Solicitar correção de emissão ou revisar parametrização de impostos no ERP.
*/

select 
    chave_acesso,
    n_Item,
    vProd,
    vICMS,
    vBC_icms,
    -- Validação ICMS (18%)
    round(vBC_icms * 0.18, 2) as icms_teorico,
    round(vICMS - (vBC_icms * 0.18), 2) as diff_icms,
    -- Validação PIS (1.65%)
    round(vBC_pis * 0.0165, 2) as pis_teorico,
    round(vPIS - (vBC_pis * 0.0165), 2) as diff_pis
from {{ ref('fct_itens_notas_fiscais') }}
where abs(vICMS - (vBC_icms * 0.18)) > 0.10 
   or abs(vPIS - (vBC_pis * 0.0165)) > 0.10