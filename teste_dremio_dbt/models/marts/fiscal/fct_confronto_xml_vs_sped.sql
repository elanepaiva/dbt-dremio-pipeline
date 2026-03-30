with xml_sum as (
    select 
        chave_nfe,
        sum(vProd) as vprod_xml,
        sum(vICMS) as vicms_xml
    from {{ ref('stg_prod') }}
    group by 1
),
sped as (
    select 
        chave_nfe,
        vProd_sped,
        vICMS_sped
    from {{ ref('stg_c100') }}
)

select 
    coalesce(x.chave_nfe, s.chave_nfe) as chave_nfe,
    coalesce(x.vprod_xml, 0) as vprod_xml,
    coalesce(s.vprod_sped, 0) as vprod_sped,
    (coalesce(x.vprod_xml, 0) - coalesce(s.vprod_sped, 0)) as dif_prod,
    
    case 
        when x.chave_nfe is null then 'Falta no MySQL'
        when s.chave_nfe is null then 'Falta no SPED'
        when abs(coalesce(x.vprod_xml, 0) - coalesce(s.vprod_sped, 0)) > 0.01 then 'Divergência'
        else 'OK'
    end as status_auditoria
from xml_sum x
full outer join sped s on x.chave_nfe = s.chave_nfe