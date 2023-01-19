{{ config(
    materialized = "table"
)}}

WITH CLIENTE AS (
    SELECT
        1 as idCliente,
        'Cliente 1' as Cliente
)

SELECT * FROM CLIENTE