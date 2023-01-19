{{ config(
    materialized = "table"
)}}

with area AS (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo','area') }}
), 

trabajador AS (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo','trabajador') }}
)

select distinct 
    CONCAT(trabajador.Nombre, ' ', trabajador.ApePaterno, ' ', trabajador.ApeMaterno) as Trabajador, 
    area.Departamento as Area, 
    trabajador.IdTrabajador as IdTrabajador
from trabajador 
inner join area using (IdArea)