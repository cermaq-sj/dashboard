import sys
import os

# Add src to path
sys.path.append(os.path.abspath(r'C:\Users\hecto\OneDrive\Escritorio\antigravity\dashboard\src'))

from db_manager import DBManager

def test_mediciones():
    print("Initializing DB...")
    db = DBManager()
    
    file_path = r'C:\Users\hecto\OneDrive\Escritorio\antigravity\dashboard\test\Mediciones.xlsx'
    
    with open(file_path, 'rb') as f:
        print(f"Ingesting file: {file_path}")
        db.ingest_mediciones_data(f)
        
    print("\n--- Metadata ---")
    meta = db.get_mediciones_metadata()
    for sheet, data in meta.items():
        print(f"Sheet: {sheet}")
        print(f"  Places: {data['places']}")
        #print skipped columns # only print 5 for brevity
        
    print("\n--- Testing Smolt Data ---")
    if 'Smolt' in meta:
        filters = {
            'mediciones_vars': [col for col in meta['Smolt']['columns'] if 'S1' in col ][:1], # test first S1 col
            'mediciones_places': meta['Smolt']['places'] if meta['Smolt']['places'] else ['General']
        }
        df = db.get_mediciones_chart_data(filters)
        print(f"Got {len(df)} rows for Smolt.")
        print(df.head(2))

    print("\n--- Testing Metales Data ---")
    if 'Metales' in meta:
        first_place = meta['Metales']['places'][0] if meta['Metales']['places'] else 'General'
        first_col = meta['Metales']['columns'][0] if meta['Metales']['columns'] else None
        
        if first_col:
            filters = {
                'mediciones_vars': [first_col],
                'mediciones_places': [first_place]
            }
            print(f"Querying: Var='{first_col}', Place='{first_place}'")
            df = db.get_mediciones_chart_data(filters)
            print(f"Got {len(df)} rows for Metales.")
            print("Columns we got back:", df.columns.tolist())
            print(df.head(2))

if __name__ == '__main__':
    test_mediciones()
