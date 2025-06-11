
  
    

  create  table
    `silver_db`.`kpi_margen_bruto__dbt_tmp`
    
    
      as
    
    (
      

WITH productos_con_precios AS (
    SELECT 
        codigo,
        descripcion,
        valor_compra,
        lista_1 as precio_venta,
        CASE 
            WHEN lista_1 > 0 
            THEN ROUND(((lista_1 - valor_compra) / lista_1) * 100, 2)
            ELSE 0 
        END as margen_bruto_pct,
        lista_1 - valor_compra as margen_bruto_absoluto
    FROM `silver_db`.`productos_master`
    WHERE lista_1 > 0 AND valor_compra > 0
)

SELECT 
    codigo,
    descripcion,
    valor_compra,
    precio_venta,
    margen_bruto_pct,
    margen_bruto_absoluto,
    CASE 
        WHEN margen_bruto_pct < 20 THEN 'BAJO'
        WHEN margen_bruto_pct BETWEEN 20 AND 40 THEN 'MEDIO'
        ELSE 'ALTO'
    END as clasificacion_margen
FROM productos_con_precios
ORDER BY margen_bruto_pct DESC
    )

  