{{ config(
    materialized = "table"
)}}

with r as (
    select
        *
    from {{ source('casa_andina_dm_dbo', 'reservacion') }}
),

dih as (
    select
        *
    from {{ source('casa_andina_dm_dbo', 'detalle_incidencia_habitacion') }}
),

dimTiempo as (
    select distinct
        EXTRACT( YEAR FROM r.fechaentrada) AS Anual,
        CONCAT(EXTRACT(YEAR FROM r.fechaentrada), '-', IF(EXTRACT(MONTH FROM r.fechaentrada)<7, 'SEMESTRE 1','SEMESTRE 2')) AS Semestre,
        CONCAT(EXTRACT(YEAR FROM r.fechaentrada), '-', EXTRACT(QUARTER FROM r.fechaentrada)) AS Trimestre,
        CAST(r.fechaentrada AS STRING FORMAT 'MONTH') AS Mes,
        EXTRACT(MONTH FROM r.fechaentrada) AS NroMes,
        CAST(r.fechaentrada as date) AS IdFecha
    from r
    union distinct
    select distinct
        EXTRACT(YEAR FROM dih.fechareportada) AS Anual,
        CONCAT(EXTRACT(YEAR FROM dih.fechareportada), '-', IF(EXTRACT(MONTH FROM dih.fechareportada)<7, 'SEMESTRE 1','SEMESTRE 2')) AS Semestre,
        CONCAT(EXTRACT(YEAR FROM dih.fechareportada), '-', EXTRACT(QUARTER FROM dih.fechareportada)) AS Trimestre,
        CAST(dih.fechareportada AS STRING FORMAT 'MONTH') AS Mes,
        EXTRACT(MONTH FROM dih.fechareportada) AS NroMes,
        CAST(dih.fechareportada as date) AS IdFecha
    from dih
)

select 
    ROW_NUMBER() OVER (ORDER BY idFecha) AS keyTiempo,
    *
from dimTiempo
