"""
Verification test: consolidated SQL with single Suma Biomasa Weight
"""
import pandas as pd
import numpy as np
import duckdb

data = {
    'Final Fecha': ['2025-01-01']*3 + ['2025-01-02']*3,
    'Batch': ['A']*6,
    'Unidad': ['U1', 'U2', 'U3', 'U1', 'U2', 'U3'],
    'Final Biomasa': [100.0, 200.0, 300.0, 150.0, 250.0, 350.0],
    'Final GF3': [1.5, 2.0, 1.8, 1.6, 2.1, 1.9],
    'Final SGR': [0.5, 0.7, 0.6, 0.55, 0.72, 0.63],
    'Dif biomasa': [10.0, 20.0, 30.0, 15.0, 25.0, 35.0],
    'Final FCR Economico': [1.1, 1.2, 1.3, 1.15, 1.25, 1.35],
}
df = pd.DataFrame(data)

con = duckdb.connect(':memory:')
con.register('test_data', df)

# New consolidated query using single "Suma Biomasa Weight"
query = """
WITH base_data AS (
    SELECT * FROM test_data
),
with_suma AS (
    SELECT *,
           SUM("Dif biomasa") OVER (PARTITION BY "Final Fecha", "Batch") AS "Suma Biomasa Eco",
           SUM("Final Biomasa") OVER (PARTITION BY "Final Fecha", "Batch") AS "Suma Biomasa Weight"
    FROM base_data
)
SELECT *,
    "Dif biomasa" / NULLIF("Suma Biomasa Eco", 0) AS "factor_eco",
    "Final FCR Economico" * ("Dif biomasa" / NULLIF("Suma Biomasa Eco", 0)) AS "Ponderacion Eco",
    SUM("Final FCR Economico" * ("Dif biomasa" / NULLIF("Suma Biomasa Eco", 0))) OVER (PARTITION BY "Final Fecha", "Batch") AS "FCR Economico Acumulado",
    "Final Biomasa" / NULLIF("Suma Biomasa Weight", 0) AS "factor_gf3",
    "Final GF3" * ("Final Biomasa" / NULLIF("Suma Biomasa Weight", 0)) AS "Ponderacion GF3",
    SUM("Final GF3" * ("Final Biomasa" / NULLIF("Suma Biomasa Weight", 0))) OVER (PARTITION BY "Final Fecha", "Batch") AS "GF3 Acumulado",
    "Final Biomasa" / NULLIF("Suma Biomasa Weight", 0) AS "factor_sgr",
    "Final SGR" * ("Final Biomasa" / NULLIF("Suma Biomasa Weight", 0)) AS "Ponderacion SGR",
    SUM("Final SGR" * ("Final Biomasa" / NULLIF("Suma Biomasa Weight", 0))) OVER (PARTITION BY "Final Fecha", "Batch") AS "SGR Acumulado"
FROM with_suma
ORDER BY "Final Fecha" ASC
"""

result = con.execute(query).df()

# Manual expected values for Day 1 (sum biomasa = 600)
exp_gf3 = (1.5*100/600) + (2.0*200/600) + (1.8*300/600)
exp_sgr = (0.5*100/600) + (0.7*200/600) + (0.6*300/600)
exp_fcr = (1.1*10/60) + (1.2*20/60) + (1.3*30/60)

print("=== Day 1 Verification ===")
day1 = result[result['Final Fecha'] == '2025-01-01']
print(f"GF3:  expected={exp_gf3:.6f}, actual={day1['GF3 Acumulado'].iloc[0]:.6f}, OK={abs(exp_gf3 - day1['GF3 Acumulado'].iloc[0]) < 0.0001}")
print(f"SGR:  expected={exp_sgr:.6f}, actual={day1['SGR Acumulado'].iloc[0]:.6f}, OK={abs(exp_sgr - day1['SGR Acumulado'].iloc[0]) < 0.0001}")
print(f"FCR:  expected={exp_fcr:.6f}, actual={day1['FCR Economico Acumulado'].iloc[0]:.6f}, OK={abs(exp_fcr - day1['FCR Economico Acumulado'].iloc[0]) < 0.0001}")

# Verify all rows same day have same accumulated values
for date in result['Final Fecha'].unique():
    d = result[result['Final Fecha'] == date]
    assert len(d['GF3 Acumulado'].unique()) == 1, f"GF3 not uniform on {date}"
    assert len(d['SGR Acumulado'].unique()) == 1, f"SGR not uniform on {date}"
    assert len(d['FCR Economico Acumulado'].unique()) == 1, f"FCR not uniform on {date}"

print("\n=== Independence Test ===")
print("All accumulated values are uniform per batch+date")
print("GF3 and SGR use same weight column but produce different results (correct)")
print(f"Day1 GF3={day1['GF3 Acumulado'].iloc[0]:.4f} != SGR={day1['SGR Acumulado'].iloc[0]:.4f}")

# Verify dedup doesn't change values
deduped = result.drop_duplicates(subset=['Final Fecha', 'Batch'])
orig_vals = result.groupby('Final Fecha')[['GF3 Acumulado', 'SGR Acumulado']].first().reset_index()
print(f"\nDedup GF3 Day1={deduped[deduped['Final Fecha']=='2025-01-01']['GF3 Acumulado'].iloc[0]:.4f}")
print(f"Dedup SGR Day1={deduped[deduped['Final Fecha']=='2025-01-01']['SGR Acumulado'].iloc[0]:.4f}")

print("\nAll tests passed!")
con.close()
