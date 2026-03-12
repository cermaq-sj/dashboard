import pandas as pd
import json
import plotly.io as pio
from src.db_manager import DatabaseManager
from src.visualizations import create_main_chart

def test_multi_axis():
    print("Initializing DB...")
    db = DatabaseManager('test_dashboard.db')
    try:
        db.ingest_mediciones_data('dashboard/test/Mediciones.xlsx')
    except Exception as e:
        db.ingest_mediciones_data('test/Mediciones.xlsx')
        
    filters = {
        'mediciones_vars': ['Al [μg/L]', 'pH'], # Different scales
        'mediciones_places': ['FF'],
        'mediciones_avg': False
    }
    
    print("Querying Data...")
    df = db.get_mediciones_chart_data(filters)
    
    print("Generating Chart with Independent Axes...")
    fig = create_main_chart(
        df, 
        variables=filters['mediciones_vars'], 
        chart_type='Líneas',
        unite_variables=True,
        independent_axes=True
    )
    
    # Verify Layout contains yaxis and yaxis2
    layout_json = fig.layout.to_plotly_json()
    print("--- Layout Check ---")
    print(f"Has yaxis: {'yaxis' in layout_json}")
    print(f"Has yaxis2: {'yaxis2' in layout_json}")
    if 'yaxis2' in layout_json:
        print(f"yaxis2 config: {layout_json['yaxis2']}")
        
    print("--- Trace Check ---")
    for i, trace in enumerate(fig.data):
        print(f"Trace {i} ({trace.name}): yaxis mapping = {trace.yaxis}")

if __name__ == '__main__':
    test_multi_axis()
