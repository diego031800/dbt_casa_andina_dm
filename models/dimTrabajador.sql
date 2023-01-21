{{ config(
    materialized = "table"
)}}

with area AS (
    select
        *
    from {{ source('casa_andina_dm_dbo','area') }}
), 

trabajador AS (
    select
        *
    from {{ source('casa_andina_dm_dbo','trabajador') }}
)

select distinct 
    ROW_NUMBER() OVER (ORDER BY trabajador.IdTrabajador) AS keyTrabajador,
    CONCAT(trabajador.Nombre, ' ', trabajador.ApePaterno, ' ', trabajador.ApeMaterno) as Trabajador, 
    area.Departamento as Area, 
    trabajador.IdTrabajador as IdTrabajador
from trabajador 
inner join area using (IdArea)