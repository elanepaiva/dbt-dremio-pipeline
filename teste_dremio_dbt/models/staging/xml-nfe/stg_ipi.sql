select
    IPI_Id as id,
    IPI_nItem as item,
    cast(nullif(IPI_vIPI, '') as double) as vIPI,
    IPI_CST as cst_ipi
from {{ source('mysql_xml', 'ipi') }}