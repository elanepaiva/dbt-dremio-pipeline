select
    cast(i.ide_Id as varchar) as chave_nfe, 
    cast(p.prod_nItem as int) as nItem,
    cast(nullif(p.prod_vProd, '') as double) as vProd,
    p.prod_Id,
    -- Precisamos trazer o valor do imposto aqui!
    cast(nullif(ic.icms_vICMS, '') as double) as vICMS 
from {{ source('mysql_xml', 'prod') }} p
-- Tente mudar o p.ide_Id para o nome da coluna que faz a ligação no seu banco
inner join {{ source('mysql_xml', 'ide') }} i on p.prod_Id = i.ide_Id
left join {{ source('mysql_xml', 'icms') }} ic on p.prod_Id = ic.icms_Id