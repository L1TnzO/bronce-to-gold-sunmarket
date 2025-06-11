

SELECT 
    r.codigo_producto,
    r.descripcion,
    r.rotacion_inventario,
    r.clasificacion_rotacion,
    m.margen_bruto_pct,
    m.clasificacion_margen,
    d.dias_inventario_disponible,
    d.alerta_stock,
    c.contribucion_utilidad_total,
    c.pct_contribucion_total,
    -- Score compuesto para priorización
    CASE 
        WHEN r.rotacion_inventario >= 4 AND m.margen_bruto_pct >= 30 THEN 'ESTRELLA'
        WHEN r.rotacion_inventario >= 4 AND m.margen_bruto_pct < 30 THEN 'VACA_LECHERA'  
        WHEN r.rotacion_inventario < 2 AND m.margen_bruto_pct >= 30 THEN 'INCOGNITA'
        ELSE 'PERRO'
    END as matriz_bcg
FROM `silver_db`.`kpi_rotacion_inventario` r
LEFT JOIN `silver_db`.`kpi_margen_bruto` m ON r.codigo_producto = m.codigo
LEFT JOIN `silver_db`.`kpi_dias_inventario` d ON r.codigo_producto = d.codigo_producto
LEFT JOIN (
    SELECT codigo_producto, 
           SUM(contribucion_utilidad_total) as contribucion_utilidad_total,
           AVG(pct_contribucion_total) as pct_contribucion_total
    FROM `silver_db`.`kpi_contribucion_utilidad`
    GROUP BY codigo_producto
) c ON r.codigo_producto = c.codigo_producto