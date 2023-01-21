{{ config(
    materialized = "table"
)}}

with a as(
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'adicional') }}
),

sa as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'multitabla') }}
)

select
    ROW_NUMBER() OVER (ORDER BY a.IdServicioAdicional) AS keyAdicional,
    a.ServicioAdicional as Adicional, 
    sa.descripcion as TipoAdicional, 
    a.IdServicioAdicional as IdServicioAdicional
from a
inner join sa on a.IdTipoAdicional = sa.valor and sa.Idtabla = '0004'