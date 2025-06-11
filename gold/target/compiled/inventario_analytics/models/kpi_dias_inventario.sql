

WITH ventas_diarias AS (
    SELECT
        codigo_producto,
        descripcion,
        DATE(fecha) as fecha_venta,
        SUM(cantidad) as cantidad_vendida_dia
    FROM `silver_db`.`ventas_limpias`
    WHERE tipo_transaccion = 'VENTA'
    GROUP BY codigo_producto, descripcion, DATE(fecha)
),
promedio_ventas AS (
    SELECT
        codigo_producto,
        descripcion,
        AVG(cantidad_vendida_dia) as venta_promedio_diaria,
        COUNT(DISTINCT fecha_venta) as dias_con_ventas
    FROM ventas_diarias
    GROUP BY codigo_producto, descripcion
),
calculo_dias AS (
    SELECT
        p.codigo as codigo_producto,
        p.descripcion,
        p.bod_1 as stock_actual,
        COALESCE(v.venta_promedio_diaria, 0) as venta_promedio_diaria,
        v.dias_con_ventas,
        CASE
            WHEN v.venta_promedio_diaria > 0
            THEN ROUND(p.bod_1 / v.venta_promedio_diaria, 0)
            ELSE 999
        END as dias_inventario_disponible
    FROM `silver_db`.`productos_master` p
    LEFT JOIN promedio_ventas v ON p.codigo = v.codigo_producto
    WHERE p.bod_1 > 0
)
SELECT
    codigo_producto,
    descripcion,
    stock_actual,
    venta_promedio_diaria,
    dias_con_ventas,
    dias_inventario_disponible,
    CASE
        WHEN dias_inventario_disponible > 90 THEN 'EXCESO'
        WHEN dias_inventario_disponible BETWEEN 30 AND 90 THEN 'NORMAL'
        WHEN dias_inventario_disponible BETWEEN 7 AND 29 THEN 'BAJO'
        WHEN dias_inventario_disponible < 7 THEN 'CRITICO'
        ELSE 'SIN_VENTAS'
    END as alerta_stock
FROM calculo_dias
ORDER BY dias_inventario_disponible ASC