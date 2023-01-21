{{ config(
    materialized = "table"
)}}

with h as (
    select
        *
    from {{ source('casa_andina_dm_dbo', 'habitacion') }}
),

th as (
    select
        *
    from {{ source('casa_andina_dm_dbo', 'multitabla') }}
)

select distinct
    ROW_NUMBER() OVER (ORDER BY h.IdHabitacion) AS keyHabitacion,
    h.Descripcion AS Habitacion,
    th.nombre as TipoHabitacion,
    h.IdHabitacion as IdHabitacion
from h
inner join th on th.valor = h.IdTipoHabitacion and th.IdTabla = '0002'
