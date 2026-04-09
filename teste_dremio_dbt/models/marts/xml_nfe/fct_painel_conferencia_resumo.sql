/* OBJETIVO: Visão executiva e totalizadora de valores de Base de Cálculo e Impostos do banco.
   SE VAZIA: Processamento falhou ou base de dados sem movimentação.
   SE CHEIA: Resumo financeiro rápido para conferência com balancete ou GIA.
   AÇÃO: Validar o campo 'status_auditoria' para garantir que o imposto total não supera a base.
*/


with base as (
    select * from {{ ref('fct_itens_notas_fiscais') }}
),
totais as (
    select
        count(distinct chave_acesso) as total_xmls,
        sum(vProd) as total_produtos_banco,
        sum(vICMS) as total_icms_banco,
        sum(vBC_icms) as total_bc_icms_banco,
        sum(vCOFINS) as total_cofins_banco
    from base
)
select 
    total_xmls,
    total_produtos_banco,
    total_icms_banco,
    total_bc_icms_banco,
    total_cofins_banco,
    -- Usando texto simples para evitar erro de encoding no Dremio
    case 
        when total_icms_banco > total_bc_icms_banco then 'ERRO: ICMS MAIOR QUE BC'
        else 'SUCESSO: VALORES OK'
    end as status_auditoria
from totais