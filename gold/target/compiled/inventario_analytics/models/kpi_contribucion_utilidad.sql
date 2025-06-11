

WITH ventas_con_margen AS (
    SELECT 
        v.codigo_producto,
        v.descripcion,
        v.precio,
        v.cantidad,
        v.total as ingresos,
        p.valor_compra,
        (v.precio - p.valor_compra) as margen_unitario,
        (v.precio - p.valor_compra) * v.cantidad as contribucion_utilidad,
        EXTRACT(YEAR_MONTH FROM v.fecha) as periodo_mes
    FROM `silver_db`.`ventas_limpias` v
    LEFT JOIN `silver_db`.`productos_master` p ON v.codigo_producto = p.codigo
    WHERE v.tipo_transaccion = 'VENTA' AND p.codigo IS NOT NULL
)

SELECT 
    codigo_producto,
    descripcion,
    periodo_mes,
    SUM(cantidad) as unidades_vendidas,
    SUM(ingresos) as ingresos_totales,
    AVG(margen_unitario) as margen_unitario_promedio,
    SUM(contribucion_utilidad) as contribucion_utilidad_total,
    ROUND(SUM(contribucion_utilidad) / SUM(SUM(contribucion_utilidad)) OVER() * 100, 2) as pct_contribucion_total
FROM ventas_con_margen
GROUP BY codigo_producto, descripcion, periodo_mes
ORDER BY contribucion_utilidad_total DESC