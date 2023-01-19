{{ config(
    materialized = "table"
)}}

with s as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo','sede') }}
),

ts as (
    select 
        *
    from {{ source('azure_sql_db_casa_andina_dbo','multitabla') }}
)

select distinct
    ROW_NUMBER() OVER (ORDER BY s.IdSede) AS keySede,
    s.Hotel as Sede,
    ts.nombre as Categoria,
    s.IdSede as IdSede,
from s
inner join ts on ts.valor = s.IdTipoSede
