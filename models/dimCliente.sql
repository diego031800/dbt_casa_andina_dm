{{ config(
    materialized = "table"
)}}

with DEPARTAMENTO as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'DEPARTAMENTO') }}
),

PROVINCIA as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'PROVINCIA') }}
),

DISTRITO as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'DISTRITO') }}
), 

CLIENTE as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'CLIENTE') }}
)

select distinct
    CLIENTE.Nombre as Cliente, 
    CLIENTE.Sexo as Genero,
    DISTRITO.Descripcion as Distrito, 
    PROVINCIA.Descripcion as Provincia, 
	DEPARTAMENTO.Descripcion as Departamento, 
    CLIENTE.IdCliente
from CLIENTE
inner join DISTRITO using (IdDistrito)
inner join PROVINCIA using (IdProvincia)
inner join DEPARTAMENTO using (IdDepartamento) 