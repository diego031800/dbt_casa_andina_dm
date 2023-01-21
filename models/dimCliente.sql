{{ config(
    materialized = "table"
)}}

with departamento as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'departamento') }}
),

provincia as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'provincia') }}
),

distrito as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'distrito') }}
), 

cliente as (
    select 
        *
    from {{ source('casa_andina_dm_dbo', 'cliente') }}
),

dimCliente as (
    select distinct
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
)

select
    ROW_NUMBER() OVER (ORDER BY dimCliente.IdCliente) AS keyCliente,
    *
from dimCliente