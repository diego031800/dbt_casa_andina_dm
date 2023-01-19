{{ config(
    materialized = "table"
)}}

with departamento as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'departamento') }}
),

provincia as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'provincia') }}
),

distrito as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'distrito') }}
), 

cliente as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'cliente') }}
)

select distinct
    ROW_NUMBER() OVER (ORDER BY cliente.IdCliente) AS keyCliente,
    cliente.Nombre as Cliente, 
    cliente.Sexo as Genero,
    distrito.Descripcion as Distrito, 
    provincia.Descripcion as Provincia, 
	departamento.Descripcion as Departamento, 
    cliente.IdCliente as IdCliente
from cliente
inner join distrito using (IdDistrito)
inner join provincia using (IdProvincia)
inner join departamento using (IdDepartamento) 