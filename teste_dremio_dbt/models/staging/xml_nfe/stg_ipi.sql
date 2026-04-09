select
    IPI_Id as nfe_id,
    IPI_nItem as n_item,
    cast(nullif(IPI_vIPI, '') as double) as vIPI,
    IPI_CST as cst_ipi
from {{ source('mysql_xml', 'ipi') }}