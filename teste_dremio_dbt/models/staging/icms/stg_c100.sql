select
    cast(file_id as varchar) as file_id,
    cast(line_n as int) as line_n,
    cast(chv_nfe_c100_9 as varchar) as chave_acesso,
    cast(num_doc_c100_8 as varchar) as numero_nota,
    cast(ser_c100_7 as varchar) as serie,
    cast(cod_part_c100_4 as varchar) as id_participante,
    -- Aqui estava o erro: certifique-se de que NÃO existe outra linha chamando data_emissao
    cast(dt_doc_c100_10 as date) as data_emissao, 
    cast(vl_doc_c100_12 as double) as valor_total_nota,
    cast(vl_bc_icms_c100_21 as double) as valor_base_icms,
    cast(vl_icms_c100_22 as double) as valor_icms_sped
from {{ source('gcp_sped', "C100.parquet") }}