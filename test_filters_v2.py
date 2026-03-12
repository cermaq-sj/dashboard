import pandas as pd
from src.db_manager import DBManager
import duckdb

def test_filters():
    print("Initialize DB...")
    db = DBManager()
    
    # Create sample data
    # Note: DuckDB ingest expects numeric types to match. 
    # Our data creation should be clean.
    dates = pd.date_range(start='2024-01-01', periods=10)
    data = {
        'Fecha': dates,
        'Lote': ['Lote A'] * 5 + ['Lote B'] * 5,
        'Departamento': ['Dep 1'] * 5 + ['Dep 2'] * 5,
        'Unidad': ['U1', 'U2', 'U3', 'U4', 'U5'] * 2,
        'Cantidad': [100.0, 200.0, 300.0, 400.0, 500.0, 100.0, 200.0, 300.0, 400.0, 500.0],
        'Peso Promedio': [1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5]
    }
    df = pd.DataFrame(data)
    
    # Initial ingest
    db.ingest_data(df)
    
    print("\n1. Test Unique Values:")
    lotes = db.get_unique_values("Lote")
    print(f"Lotes found: {lotes}")
    # DuckDB returns tuples sometimes or list depending on fetch logic. 
    # Check if 'Lote A' is in the result.
    if 'Lote A' in lotes:
        print("PASS: Lote A found")
    else:
        print("FAIL: Lote A not found")

    # Add extra row for aggregation test
    # Same date as index 0 (2024-01-01), same Lote A.
    extra_row = pd.DataFrame({
        'Fecha': [pd.Timestamp('2024-01-01')],
        'Lote': ['Lote A'],
        'Departamento': ['Dep 1'],
        'Unidad': ['U2_Duplicate'],
        'Cantidad': [50.0],
        'Peso Promedio': [1.2]
    })
    
    # Re-ingest (append logic not really supported by ingest, it replaces).
    # So construct full DF first.
    full_df = pd.concat([df, extra_row], ignore_index=True)
    db.ingest_data(full_df)
    
    print("\n2. Test Aggregation (Sum Units):")
    # Criteria: Lote A, Date 2024-01-01.
    # Rows matching: 
    # 1. Original Index 0: U1, Cant 100, Wt 1.1
    # 2. Extra Row: U2_Dupe, Cant 50, Wt 1.2
    # Expected Aggregation: Cant = 150, Wt = Avg(1.1, 1.2) = 1.15
    
    filters = {
        'batches': ['Lote A'],
        'date_range': (pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-01')),
        'sum_units': True,
        'variables': ['Cantidad', 'Peso Promedio']
    }
    
    res = db.get_filtered_data(filters)
    print("Result:")
    print(res)
    
    if len(res) == 1:
        cant = res.iloc[0]['Cantidad']
        peso = res.iloc[0]['Peso Promedio']
        print(f"Got Cant: {cant}, Peso: {peso}")
        
        if cant == 150.0 and abs(peso - 1.15) < 0.001:
            print("PASS: Aggregation logic correct (Sum/Avg).")
        else:
            print("FAIL: Values mismatch.")
    else:
        print(f"FAIL: Expected 1 row, got {len(res)}")

if __name__ == "__main__":
    test_filters()
