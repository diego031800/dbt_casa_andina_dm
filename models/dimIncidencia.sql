{{ config(
    materialized = "table"
)}}

with i as (
    select
        *
    from {{ source('casa_andina_dm_dbo', 'incidencia') }}
),

m as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'multitabla') }}
)

select 
    ROW_NUMBER() OVER (ORDER BY i.IdIncidencia) AS keyIncidencia,
    i.Incidencia as Incidencia,
    m.descripcion as TipoIncidencia, 
    i.IdIncidencia as IdIncidencia
from i 
inner join m on i.IdTipoIncidencia = m.valor and m.Idtabla = '0005'