{{ config(
    materialized = "table"
)}}

with r as (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'reservacion') }}
),

dih as (
    select
        *
    from {{ source('azure_sql_db_casa_andina_dbo', 'detalle_incidencia_habitacion') }}
)

select distinct 
    ROW_NUMBER() OVER (ORDER BY r.FechaEntrada) AS keyTiempo,
    EXTRACT( YEAR FROM r.FechaEntrada) AS Anual,
	CONCAT(EXTRACT(YEAR FROM r.FechaEntrada), '-', IF(EXTRACT(MONTH FROM r.FechaEntrada)<7, 'SEMESTRE 1','SEMESTRE 2')) AS Semestre,
	CONCAT(EXTRACT(YEAR FROM r.FechaEntrada), '-', EXTRACT(QUARTER FROM r.FechaEntrada)) AS Trimestre,
	EXTRACT(MONTH FROM r.FechaEntrada) AS Mes,
	EXTRACT(MONTH FROM r.FechaEntrada) AS NroMes,
	CAST(r.FechaEntrada as date) AS IdFecha
from r
/* union all
(select distinct
    ROW_NUMBER() OVER (ORDER BY dih.FechaReportada),
    EXTRACT(YEAR FROM dih.FechaReportada),
	CONCAT(EXTRACT(YEAR FROM dih.FechaReportada), '-', IF(EXTRACT(MONTH FROM dih.FechaReportada)<7, 'SEMESTRE 1','SEMESTRE 2')),
	CONCAT(EXTRACT(YEAR FROM dih.FechaReportada), '-', EXTRACT(QUARTER dih.FechaReportada)),
	EXTRACT(MONTH FROM  dih.FechaReportada),
	EXTRACT(MONTH FROM dih.FechaReportada),
	CAST(dih.FechaReportada as date)
from dih) */