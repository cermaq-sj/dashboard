from src.db_manager import DBManager
import pandas as pd

def debug_filters():
    print("Initialize DB...")
    db = DBManager()
    
    # Create sample data with English names to test resolution
    df = pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', periods=5),
        'Batch': ['Lote A', 'Lote A', 'Lote B', 'Lote B', 'Lote C'],
        'Count': [100.0, 100.0, 200.0, 200.0, 300.0],
        'Avg Weight': [1.1, 1.2, 1.3, 1.4, 1.5]
    })
    
    print("Ingesting data...")
    db.ingest_data(df)
    
    print("\n1. Check Schema:")
    try:
        desc = db.query("DESCRIBE fishtalk_data")
        print(desc)
    except Exception as e:
        print(f"Error describing table: {e}")
        
    print("\n2. Check Unique Lote:")
    lotes = db.get_unique_values("Lote")
    print(f"Lotes found: {lotes}")
    
    # Verify variable logic simulation
    print("\n3. Verify Variable Logic:")
    all_cols = ['Fecha', 'Lote', 'Cantidad', 'Peso Promedio']
    
    # Mimic the logic in filters.py
    try:
        col_type_map = {row['column_name']: row['column_type'] for _, row in desc.iterrows()}
        print(f"Col Map: {col_type_map}")
    except:
        col_type_map = {}
        
    numeric_types = ['DOUBLE', 'FLOAT', 'DECIMAL', 'BIGINT', 'INTEGER', 'INT', 'HUGEINT', 'SMALLINT', 'TINYINT', 'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT']

    for col in all_cols:
        ctype = col_type_map.get(col, '').upper()
        if not ctype:
             # Try case insensitive match for map
             # In DuckDB describe results are usually lowercase or match creation?
             # Let's check what 'desc' actually has.
             pass
        is_numeric = any(t in ctype for t in numeric_types)
        print(f"Col: {col}, Type: {ctype}, Is Numeric: {is_numeric}")

if __name__ == "__main__":
    debug_filters()
