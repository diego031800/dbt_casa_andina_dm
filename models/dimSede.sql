{{ config(
    materialized = "table"
)}}

with s as (
    select 
        *
    from {{ source('casa_andina_dm_dbo','sede') }}
),

ts as (
    select 
        *
    from {{ source('casa_andina_dm_dbo','multitabla') }}
)

select distinct
    ROW_NUMBER() OVER (ORDER BY s.IdSede) AS keySede,
    s.Hotel as Sede,
    ts.nombre as Categoria,
    s.IdSede as IdSede,
from s
inner join ts on ts.valor = s.IdTipoSede and ts.IdTabla = '0001'
