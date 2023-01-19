{{ config(
    materialized = "table"
)}}

with AREA AS (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo','AREA') }}
), 

TRABAJADOR AS (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo','TRABAJADOR') }}
)

select distinct 
    CONCAT(TRABAJADOR.Nombre, ' ', TRABAJADOR.ApePaterno, ' ', TRABAJADOR.ApeMaterno) as Trabajador, 
    AREA.Departamento as Area, 
    TRABAJADOR.IdTrabajador as IdTrabajador
from TRABAJADOR 
inner join AREA using (IdArea)