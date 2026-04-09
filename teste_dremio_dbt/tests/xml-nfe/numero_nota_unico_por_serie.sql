select
    numero_nota,
    cnpj_emitente,
    count(distinct chave_acesso) as qtd_chaves
from {{ ref('fct_itens_notas_fiscais') }}
group by 1, 2
having count(distinct chave_acesso) > 1