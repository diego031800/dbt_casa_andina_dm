{{ config(
    materialized = "table"
)}}

with dih as (
    select
        *
    from {{ source('casa_andina_dm_dbo', 'detalle_incidencia_habitacion') }}
),

dh as (
    select 
        *
    from {{ source('dm', 'dimHabitacion') }}
),

di as (
    select 
        *
    from {{ source('dm', 'dimIncidencia') }}
),

dt as (
    select 
        *
    from {{ source('dm', 'dimTiempo') }}
),

dtt as (
    select 
        *
    from {{ source('dm', 'dimTrabajador') }}
),

s as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'sede') }}
),

h as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'habitacion') }}
)

select distinct
    di.KeyIncidencia, 
    dh.KeyHabitacion, 
    dt.KeyTiempo, 
    dtt.KeyTrabajador, 
    s.IdSede as KeySede, 
	DATE_DIFF(dih.FechaSolucion, dih.FechaReportada, day) as TiempoTotal,
	count(*) as NroIncidencias
from dih
inner join di on di.IdIncidencia = dih.IdIncidencia
inner join dh on dh.IdHabitacion = dih.IdHabitacion
inner join dtt on dtt.IdTrabajador = dih.IdTrabajador
inner join dt on dt.IdFecha = CAST(dih.FechaReportada as date)
inner join h on h.IdHabitacion = dih.IdHabitacion
inner join s on s.IdSede = h.IdSede
group by di.KeyIncidencia, dh.KeyHabitacion, dt.KeyTiempo, dtt.KeyTrabajador, s.IdSede, dih.FechaReportada, dih.FechaSolucion
