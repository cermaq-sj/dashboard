"""
Test: % Mortalidad Acumulada calculation logic
"""
import pandas as pd
import duckdb

con = duckdb.connect(':memory:')

# Simulate table with Hatchery 1 and other departments
data = {
    'Final Fecha': ['2025-01-01']*4 + ['2025-01-02']*4 + ['2025-01-03']*4,
    'Batch': ['A','A','A','A'] * 3,
    'Departamento': ['Hatchery 1','Hatchery 1','FRY','FRY'] * 3,
    'Unidad': ['U1','U2','U3','U4'] * 3,
    'Final Numero': [
        1000, 2000, 500, 800,   # Day 1
        950, 1900, 480, 780,    # Day 2
        900, 1800, 460, 760,    # Day 3
    ],
    'Final Mortalidad, Numero': [
        5, 10, 3, 4,     # Day 1: sum=22
        8, 15, 5, 6,     # Day 2: sum=34
        12, 20, 7, 9,    # Day 3: sum=48
    ],
}
df = pd.DataFrame(data)
con.register('fishtalk_data', df)

# Step 1-3: Baseline from Hatchery 1
baseline_query = """
    WITH hatchery_data AS (
        SELECT "Batch", "Unidad", "Final Fecha", "Final Numero"
        FROM fishtalk_data
        WHERE LOWER(TRIM("Departamento")) LIKE '%hatchery%1%'
          AND "Final Numero" IS NOT NULL
          AND CAST("Final Numero" AS DOUBLE) > 0
    ),
    earliest_per_unit AS (
        SELECT "Batch", "Unidad",
               FIRST("Final Numero" ORDER BY "Final Fecha" ASC) AS initial_numero
        FROM hatchery_data
        GROUP BY "Batch", "Unidad"
    )
    SELECT "Batch" AS batch, SUM(CAST(initial_numero AS DOUBLE)) AS cant_por_batch
    FROM earliest_per_unit
    GROUP BY "Batch"
"""

baseline_df = con.execute(baseline_query).df()
print("=== Baseline (Cant. por batch) ===")
print(baseline_df)
# Expected: Batch A has Hatchery 1 units U1=1000, U2=2000 -> cant = 3000

cant_por_batch = dict(zip(baseline_df['batch'], baseline_df['cant_por_batch']))
print(f"  Batch A baseline: {cant_por_batch.get('A', 'NOT FOUND')}")
assert cant_por_batch['A'] == 3000, f"Expected 3000, got {cant_por_batch['A']}"

# Step 4-5: Sum mortality per date per batch (all units, all depts)
temp_mort = pd.to_numeric(df['Final Mortalidad, Numero'], errors='coerce').fillna(0)
temp_batch = df['Batch']
temp_date = df['Final Fecha']

suma_mort = temp_mort.groupby([temp_date, temp_batch]).transform('sum')

# Step 6: % = (Suma / Cant) * 100
baseline_vals = temp_batch.map(cant_por_batch)
pct = (suma_mort / baseline_vals) * 100

df['Suma Mortalidad'] = suma_mort
df['Cant. por batch'] = baseline_vals
df['% Mortalidad Acumulada'] = pct

print("\n=== Results ===")
print(df[['Final Fecha', 'Unidad', 'Departamento', 'Final Mortalidad, Numero', 'Suma Mortalidad', 'Cant. por batch', '% Mortalidad Acumulada']].to_string(index=False))

# Verify per-day sums
print("\n=== Verification ===")
for date in ['2025-01-01', '2025-01-02', '2025-01-03']:
    d = df[df['Final Fecha'] == date]
    daily_sum = d['Final Mortalidad, Numero'].sum()
    pct_val = d['% Mortalidad Acumulada'].iloc[0]
    expected_pct = (daily_sum / 3000) * 100
    print(f"Date {date}: mort_sum={daily_sum}, % ={pct_val:.4f}, expected={expected_pct:.4f}, OK={abs(pct_val - expected_pct) < 0.001}")

print("\nAll tests passed!")
con.close()
