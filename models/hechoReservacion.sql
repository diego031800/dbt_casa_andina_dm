{{ config(
    materialized = "table"
)}}

with dr as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'detalle_habitacion_reserva') }}
),

dh as (
    select 
        *
    from {{ source('dbo', 'dimHabitacion') }}
),

dc as (
    select 
        *
    from {{ source('dbo', 'dimCliente') }}
),

dt as (
    select 
        *
    from {{ source('dbo', 'dimTiempo') }}
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
),

r as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'reservacion') }}
)

select distinct
    dh.KeyHabitacion, 
    dc.KeyCliente, 
    s.IdSede as KeySede, 
    dt.KeyTiempo,
	SUM(dr.CantidadDias*dr.Precio) as MontoHabReservada,
	count(*) as NroHabReservadas
from r
inner join dr on r.IdReservacion = dr.IdReservacion
inner join dh on dh.IdHabitacion = dr.IdHabitacion
inner join dc on dc.IdCliente = r.IdCliente
inner join dt on dt.IdFecha = CAST(r.FechaEntrada as date)
inner join h on h.IdHabitacion = dr.IdHabitacion
inner join s on s.IdSede = h.IdSede
group by dh.KeyHabitacion, dc.KeyCliente, s.IdSede, dt.KeyTiempo