{{ config(
    materialized = "table"
)}}

with a as(
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'adicional') }}
),

sa as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'multitabla') }}
)

select
    ROW_NUMBER() OVER (ORDER BY a.IdServicioAdicional) AS keyAdicional,
    a.ServicioAdicional as Adicional, 
    sa.descripcion as TipoAdicional, 
    a.IdServicioAdicional as IdServicioAdicional
from a
inner join sa on a.IdTipoAdicional = sa.valor and sa.Idtabla = '0004'