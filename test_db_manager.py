import sys
import os
import pandas as pd
from src.db_manager import DBManager

def test_db():
    db = DBManager()
    db.connect()
    try:
        df = pd.read_excel('FISHTALK_DATABASE.xlsx', sheet_name='Data')
        db.ingest_data(df)
        
        # Try fetching with 1 var
        filters_1 = {'variables': ['GF3 Acumulado']}
        df_1 = db.get_filtered_data(filters_1).head(10)
        
        # Try fetching with 2 vars
        filters_2 = {'variables': ['GF3 Acumulado', 'SGR Acumulado']}
        df_2 = db.get_filtered_data(filters_2).head(10)
        
        print("Shapes 1 vs 2:", df_1.shape, df_2.shape)
        
        if 'GF3 Acumulado' in df_1.columns and 'GF3 Acumulado' in df_2.columns:
            print("GF3 (1 var) == GF3 (2 vars)?", (df_1['GF3 Acumulado'].fillna(-999) == df_2['GF3 Acumulado'].fillna(-999)).all())
            print("GF3 (1 var) head:")
            print(df_1[['Fecha', 'GF3 Acumulado']].head(3))
            print("GF3 (2 vars) head:")
            print(df_2[['Fecha', 'GF3 Acumulado', 'SGR Acumulado']].head(3))
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    test_db()
