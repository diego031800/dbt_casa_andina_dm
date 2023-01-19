{{ config(
    materialized = "table"
)}}

with dih as (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'detalle_incidencia_habitacion') }}
),

dh as (
    select 
        *
    from {{ source('dbo', 'dimHabitacion') }}
),

di as (
    select 
        *
    from {{ source('dbo', 'dimIncidencia') }}
),

dt as (
    select 
        *
    from {{ source('dbo', 'dimTiempo') }}
),

dtt as (
    select 
        *
    from {{ source('dbo', 'dimTrabajador') }}
),

s as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'sede') }}
),

h as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'habitacion') }}
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