"""Test script for KPIs y Proyecciones por Batch integration."""
import pandas as pd
import sys
sys.path.insert(0, '.')
from src.db_manager import DBManager

print("=" * 60)
print("KPIs y PROYECCIONES INTEGRATION TEST")
print("=" * 60)

# Step 1: Create DB Manager and ingest the file
print("\n1. Ingesting KPIs y Proyecciones file...")
db = DBManager()

# Simulate file-like object
file_path = 'test/KPIs y Proyecciones por Batch.xlsx'
with open(file_path, 'rb') as f:
    db.ingest_kpis_proyecciones(f)

# Step 2: Verify KPI Thresholds table
print("\n2. Verifying KPI Thresholds...")
tables = [t[0] for t in db.con.execute("SHOW TABLES").fetchall()]
assert 'kpi_thresholds' in tables, "kpi_thresholds table not found!"
kpi_df = db.con.execute("SELECT * FROM kpi_thresholds").df()
print(f"   Rows: {len(kpi_df)}")
print(f"   Columns: {kpi_df.columns.tolist()}")
print(f"   Tipos KPI: {kpi_df['tipo_kpi'].unique().tolist()}")
assert len(kpi_df) == 13, f"Expected 13 KPI rows, got {len(kpi_df)}"
print("   ✅ KPI thresholds ingested correctly")

# Step 3: Verify Proyecciones table
print("\n3. Verifying Proyecciones Data...")
assert 'proyecciones_data' in tables, "proyecciones_data table not found!"
proj_df = db.con.execute("SELECT * FROM proyecciones_data").df()
print(f"   Rows: {len(proj_df)}")
print(f"   Columns: {proj_df.columns.tolist()}")
batches = proj_df['batch'].unique().tolist()
print(f"   Batches: {batches}")
assert len(batches) == 5, f"Expected 5 batches, got {len(batches)}"
assert len(proj_df) == 5 * 1096, f"Expected {5*1096} rows, got {len(proj_df)}"
print("   ✅ Proyecciones data ingested correctly")

# Step 4: Test get_kpi_thresholds()
print("\n4. Testing get_kpi_thresholds()...")
thresholds = db.get_kpi_thresholds()
print(f"   Types found: {list(thresholds.keys())}")
assert len(thresholds) == 3, f"Expected 3 KPI types, got {len(thresholds)}"
for tipo, depts in thresholds.items():
    print(f"   {tipo}: {depts}")
print("   ✅ get_kpi_thresholds() works correctly")

# Step 5: Test get_proyecciones_metadata()
print("\n5. Testing get_proyecciones_metadata()...")
meta = db.get_proyecciones_metadata()
print(f"   Batches: {meta.get('batches', [])}")
print(f"   Variables: {meta.get('variables', [])}")
assert len(meta['batches']) == 5, f"Expected 5 batches, got {len(meta['batches'])}"
assert len(meta['variables']) >= 5, f"Expected >= 5 variables, got {len(meta['variables'])}"
print("   ✅ get_proyecciones_metadata() works correctly")

# Step 6: Test get_proyecciones_data() with filters
print("\n6. Testing get_proyecciones_data()...")
# All batches, all variables
all_proj = db.get_proyecciones_data()
print(f"   No filters: {all_proj.shape}")

# Filter by batch
batch_proj = db.get_proyecciones_data(batches=['65'])
print(f"   Batch 65 only: {batch_proj.shape}")
assert len(batch_proj) == 1096, f"Expected 1096 rows for batch 65, got {len(batch_proj)}"

# Filter by variables
var_proj = db.get_proyecciones_data(batches=['65', '66'], variables=['SGR Plan', 'FCR Plan'])
print(f"   Batch 65+66, SGR+FCR: {var_proj.shape}")
assert 'SGR Plan' in var_proj.columns, "SGR Plan column missing"
assert 'FCR Plan' in var_proj.columns, "FCR Plan column missing"
print("   ✅ get_proyecciones_data() works correctly")

# Step 7: Test chart creation with KPI lines
print("\n7. Testing chart creation with KPI thresholds...")
from src.visualizations import create_main_chart

# We need some dummy data to test
dummy_df = pd.DataFrame({
    'Final Fecha': pd.date_range('2025-01-01', periods=30, freq='D'),
    '% Mortalidad diaria': [0.03 + 0.001 * i for i in range(30)],
    'Batch': ['65'] * 30,
})

fig = create_main_chart(
    dummy_df, 
    ['% Mortalidad diaria'],
    kpi_thresholds=thresholds,
    active_kpis=['% Mortalidad diaria'],
)
# Check that hlines were added (shapes contain them)
shapes = fig.layout.shapes or []
print(f"   Shapes (hlines): {len(shapes)}")

# Check annotations for KPI labels
annotations = fig.layout.annotations or []
kpi_annotations = [a for a in annotations if 'KPI' in str(getattr(a, 'text', ''))]
print(f"   KPI Annotations: {len(kpi_annotations)}")
print("   ✅ Chart with KPI thresholds created")

# Step 8: Test chart with projection overlay
print("\n8. Testing chart creation with projection overlay...")
proj_data = db.get_proyecciones_data(batches=['65'], variables=['FCR Plan'])
fig2 = create_main_chart(
    dummy_df,
    ['% Mortalidad diaria'],
    proyecciones_df=proj_data,
)
proj_traces = [t for t in fig2.data if '(Plan)' in str(getattr(t, 'name', ''))]
print(f"   Projection traces: {len(proj_traces)}")
assert len(proj_traces) >= 1, "Expected at least 1 projection trace"
print(f"   First trace name: {proj_traces[0].name}")
print("   ✅ Chart with projection overlay created")

print("\n" + "=" * 60)
print("ALL TESTS PASSED! ✅")
print("=" * 60)
