{{ config(
    materialized = "table"
)}}

with dar as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'detalle_adicional_reservacion') }}
),

da as (
    select 
        *
    from {{ source('dbo', 'dimAdicional') }}
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

a as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'adicional') }}
),

r as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'reservacion') }}
)

select 
    da.KeyAdicional, 
    dc.KeyCliente, 
    s.IdSede as KeySede, 
    dt.KeyTiempo,
	SUM(dar.Cantidad*dar.Precio) as MontoAdicionales
from r
inner join dar on r.IdReservacion = dar.IdReservacion
inner join da on da.IdServicioAdicional = dar.IdServicioAdicional
inner join dc on dc.IdCliente = r.IdCliente
inner join dt on dt.IdFecha = CAST(r.FechaEntrada as date)
inner join a on a.IdServicioAdicional = dar.IdServicioAdicional
inner join s on s.IdSede = a.IdSede
group by da.KeyAdicional, dc.KeyCliente, s.IdSede, dt.KeyTiempo