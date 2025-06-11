from prefect import flow
from productos_master_etl import productos_master_etl
from ventas_etl import ventas_etl
from compras_etl import compras_etl
from transacciones_etl import transacciones_etl

@flow(name="ETL Principal - Bronce a Plata")
def main_etl():
    """
    Flow principal que orquesta todo el proceso ETL.
    
    Secuencia de ejecución:
    1. Productos Master - Consolida y limpia el catálogo de productos
    2. Ventas ETL - Procesa ventas con integridad referencial
    3. Compras ETL - Procesa compras con integridad referencial  
    4. Transacciones ETL - Unifica ventas y compras para análisis integral
    
    Garantiza integridad referencial y trazabilidad completa.
    """
    
    # 1. Crear productos master (base para integridad referencial)
    print("🔧 Iniciando ETL Productos Master...")
    productos_master_etl()
    print("✅ Productos Master completado")
    
    # 2. Procesar ventas con integridad referencial
    print("🔧 Iniciando ETL Ventas...")
    ventas_etl()
    print("✅ Ventas completado")
    
    # 3. Procesar compras con integridad referencial
    print("🔧 Iniciando ETL Compras...")
    compras_etl()
    print("✅ Compras completado")
    
    # 4. Unificar transacciones para análisis integral
    print("🔧 Iniciando ETL Transacciones Unificadas...")
    transacciones_etl()
    print("✅ Transacciones Unificadas completado")
    
    print("🎉 ETL Principal completado exitosamente!")
    print("📁 Archivos generados en result/:")
    print("   - productos_master.csv")
    print("   - mapeo_codigos.csv") 
    print("   - ventas_limpias.csv")
    print("   - compras_limpias.csv")
    print("   - transacciones_unificadas.csv")

if __name__ == "__main__":
    main_etl()