{{ config(materialized='table') }}

WITH ventas_agregadas AS (
    SELECT
        codigo_producto,
        descripcion,
        SUM(total) as costo_productos_vendidos,
        COUNT(*) as num_transacciones
    FROM {{ source('silver_db', 'ventas_limpias') }}
    WHERE tipo_transaccion = 'VENTA'
    GROUP BY codigo_producto, descripcion
),
inventario_actual AS (
    SELECT
        codigo,
        descripcion,
        valor_compra * bod_1 as valor_inventario_actual,
        bod_1 as stock_actual
    FROM {{ source('silver_db', 'productos_master') }}
    WHERE bod_1 > 0
),
calculo_rotacion AS (
    SELECT
        v.codigo_producto,
        v.descripcion,
        v.costo_productos_vendidos,
        i.valor_inventario_actual as inventario_promedio,
        CASE
            WHEN i.valor_inventario_actual > 0
            THEN ROUND(v.costo_productos_vendidos / i.valor_inventario_actual, 2)
            ELSE 0
        END as rotacion_inventario,
        i.stock_actual,
        v.num_transacciones
    FROM ventas_agregadas v
    LEFT JOIN inventario_actual i ON v.codigo_producto = i.codigo
    WHERE i.codigo IS NOT NULL
)
SELECT
    codigo_producto,
    descripcion,
    costo_productos_vendidos,
    inventario_promedio,
    rotacion_inventario,
    CASE
        WHEN rotacion_inventario < 2 THEN 'BAJA'
        WHEN rotacion_inventario BETWEEN 2 AND 6 THEN 'MEDIA'
        ELSE 'ALTA'
    END as clasificacion_rotacion,
    stock_actual,
    num_transacciones
FROM calculo_rotacion