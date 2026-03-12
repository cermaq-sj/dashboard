import pandas as pd
import numpy as np
import plotly.graph_objects as go
import colorsys

# Mocking the context
def get_col(df, name):
    name_lower = name.lower()
    for c in df.columns:
        if c.lower() == name_lower: return c
    for c in df.columns:
        if name_lower in c.lower(): return c
    return None

# Create dummy DF matches user's description
df = pd.DataFrame({
    'Batch': ['64SJ'] * 10,
    'Final Fecha': pd.date_range('2025-01-01', periods=10),
    'SGR': np.random.rand(10),
    'Days': range(10),
    'SeriesName': ['64SJ'] * 10
})

print("Columns:", df.columns)

# Logic from visualizations.py
hover_date_col = None
for cand in ['final fecha', 'fecha', 'date']:
    found = get_col(df, cand)
    if found:
        hover_date_col = found
        print(f"Found date col: {found}")
        break

if not hover_date_col:
    print("Date col NOT found!")

# Simulation of chart loop
series_data = df
cd_cols = []
cd_map = {}

# Days logic
days_col = get_col(df, 'days')
if days_col:
    cd_cols.append(series_data[days_col].values.astype(object))
    cd_map['days'] = len(cd_cols) - 1

# Date logic
if hover_date_col:
    cd_cols.append(series_data[hover_date_col].values.astype(object))
    cd_map['date'] = len(cd_cols) - 1

print("cd_map:", cd_map)
print("First row customdata:", [c[0] for c in cd_cols])

# Template logic
ht = ""
x_axis_mode = 'Days'
if x_axis_mode == 'Days' and 'date' in cd_map:
    ht += f"Fecha: %{{customdata[{cd_map['date']}]|%d-%m-%Y}}<br>"
    print("Template uses customdata date")
else:
    ht += f"Fecha: %{{x|%d-%m-%Y}}<br>"
    print("Template uses X")

print("HT:", ht)
