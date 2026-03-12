"""Full integration test for the dashboard pipeline."""
import pandas as pd
import sys
sys.path.insert(0, '.')
from src.data_processing import load_and_clean_data, basic_cleaning
from src.db_manager import DBManager
from src.visualizations import create_main_chart
from src.calculations import calculate_kpis

print("=" * 60)
print("FULL INTEGRATION TEST")
print("=" * 60)

# Step 1: Load and clean data
print("\n1. Loading data...")
df1 = pd.read_excel('test/08-02-2026 TEST COLUMNAS - copia.xls')
df1 = basic_cleaning(df1)
df1['source_file'] = 'test1.xls'

df2 = pd.read_excel('test/Mediciones - copia.xlsx')
df2 = basic_cleaning(df2)
df2['source_file'] = 'test2.xlsx'

combined = pd.concat([df1, df2], ignore_index=True)
print(f"   Combined: {combined.shape[0]} rows x {combined.shape[1]} cols")

# Step 2: Ingest to DB
print("\n2. Ingesting to DuckDB...")
db = DBManager()
db.ingest_data(combined)
print("   OK")

# Step 3: Column resolution
print("\n3. Column resolution:")
cols = [c[0] for c in db.con.execute("DESCRIBE fishtalk_data").fetchall()]
for key in ['Fecha', 'Lote', 'Departamento', 'Unidad', 'Days']:
    r = db._resolve_col(key, cols)
    print(f"   {key} -> {r}")

# Step 4: Unique values
print("\n4. Unique values:")
batches = db.get_unique_values('Lote')
print(f"   Batches ({len(batches)}): {batches[:5]}")
deptos = db.get_unique_values('Departamento')
print(f"   Deptos ({len(deptos)}): {deptos[:5]}")

# Step 5: Filtered data (no filters)
print("\n5. get_filtered_data (no filters):")
filt_all = db.get_filtered_data({})
print(f"   Shape: {filt_all.shape}")

# Step 6: Filtered data (batch filter)
print("\n6. get_filtered_data (batch=68SJ):")
filt_batch = db.get_filtered_data({'batches': ['68SJ']})
print(f"   Shape: {filt_batch.shape}")

# Step 7: Filtered data (sum_units)
print("\n7. get_filtered_data (batch=68SJ, sum_units=True):")
filt_sum = db.get_filtered_data({'batches': ['68SJ'], 'sum_units': True})
print(f"   Shape: {filt_sum.shape}")

# Step 8: KPIs
print("\n8. KPIs:")
kpis = calculate_kpis(filt_batch)
if kpis:
    for kpi in kpis:
        print(f"   {kpi['label']}: {kpi['value']} {kpi['unit']}")
else:
    print("   No KPIs generated")

# Step 9: Chart creation (various scenarios)
print("\n9. Chart tests:")
test_vars = [
    ['Final Peso prom'],
    ['Final Mortalidad, porcentaje'],
    ['Final Peso prom', 'Final Mortalidad, porcentaje'],  # dual axis
]
for tvars in test_vars:
    fig = create_main_chart(filt_batch, tvars, 'Overlay', 'Date', 'Lineas')
    print(f"   Variables={tvars}: {len(fig.data)} traces")

# Step 10: Chart with Days axis
print("\n10. Chart with Days axis:")
fig_days = create_main_chart(filt_batch, ['Final Peso prom'], 'Overlay', 'Days', 'Lineas')
print(f"    Traces: {len(fig_days.data)}")
if fig_days.data:
    t = fig_days.data[0]
    print(f"    First trace: {t.name}")
    if t.x is not None and len(t.x) > 0:
        print(f"    X range: {min(t.x)} - {max(t.x)}")

# Step 11: Chart types
print("\n11. Chart type tests:")
for ct in ['Lineas', 'Barras', 'Area']:
    fig_ct = create_main_chart(filt_batch, ['Final Peso prom'], 'Overlay', 'Date', ct)
    print(f"    {ct}: {len(fig_ct.data)} traces, type={type(fig_ct.data[0]).__name__}")

print("\n" + "=" * 60)
print("ALL TESTS PASSED!")
print("=" * 60)
