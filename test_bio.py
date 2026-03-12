import pandas as pd
import numpy as np
from src.db_manager import DBManager

db = DBManager()
df = pd.DataFrame({
    'Batch': ['A', 'A', 'A', 'A', 'A', 'B', 'B', 'C', 'C'],
    'Final Fecha': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03', '2023-01-04', '2023-01-01', '2023-01-02', '2023-01-01', '2023-01-02'],
    'Departamento': ['FRY', 'FRY', 'FRY', 'FRY', 'SMOLT', 'FRY', 'FRY', 'FRY', 'FRY'],
    'Unidad': ['U1', 'U1', 'U2', 'U3', 'U1', 'U4', 'U4', 'U5', 'U6'],
    'Final Biomasa': [0, 10, 15, 100, 50, 0, 20, 10, 12]
})

print('--- Input Data ---')
print(df)
db.ingest_data(df)

print('\n--- Result Data ---')
res = db.con.execute('SELECT "Batch", "Unidad", "Final Fecha", "Departamento", "Final Biomasa", "Dif biomasa" FROM fishtalk_data ORDER BY "Batch", "Unidad", "Final Fecha"').df()
print(res)
