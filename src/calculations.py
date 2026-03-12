import pandas as pd

def calculate_kpis(df):
    """
    Calculates summary metrics from the filtered DataFrame.
    Returns a list of dictionaries with {label, value, unit}.
    """
    if df.empty:
        return []
    
    kpis = []
    
    # Helper to find columns loosely
    cols = {c.lower(): c for c in df.columns}
    
    def get_col(keywords):
        for k in keywords:
            for c_lower, c_original in cols.items():
                if k in c_lower:
                    return c_original
        return None

    # 1. Biomass (Biomasa / Peso Total)
    # Strategy: Sum of the LAST value for each unit in the period.
    # Because biomass is a snapshot, not additive over time for the same unit.
    # But if we view multiple units, we sum them.
    col_bio = get_col(['biomasa', 'biomass', 'peso total', 'total weight'])
    col_unit = get_col(['unidad', 'unit', 'jaula'])
    col_date = get_col(['fecha', 'date'])
    
    if col_bio and col_unit and col_date:
        # Get latest date per unit
        latest_indices = df.groupby(col_unit)[col_date].idxmax().dropna()
        current_biomass = df.loc[latest_indices, col_bio].sum()
        kpis.append({
            "label": "Biomasa Total (Actual)",
            "value": f"{current_biomass:,.0f}",
            "unit": "kg"
        })
    elif col_bio:
        # Fallback if no unit col (e.g. pre-aggregated), just take max or sum?
        # If pre-aggregated by sum_units=True, then for each date we have total biomass.
        # We want the biomass at end of period.
        if col_date:
            latest_idx = df[col_date].idxmax()
            current_bio = df.loc[latest_idx, col_bio] # This is already sum if grouped
            kpis.append({
                "label": "Biomasa Actual",
                "value": f"{current_bio:,.0f}",
                "unit": "kg"
            })

    # 2. Mortality (Deads / Muertos) - Additive
    col_mort = get_col(['muertos', 'deads', 'mortalidad', 'mortality'])
    if col_mort:
        total_mort = df[col_mort].sum()
        kpis.append({
            "label": "Mortalidad (Periodo)",
            "value": f"{total_mort:,.0f}",
            "unit": "peces"
        })
        
    # 3. FCR (Economic / Biológico) - Average
    # Ideally cumulative FCR at end of period.
    col_fcr = get_col(['fcr', 'factor'])
    if col_fcr:
        # If we have multiple units, we should average them.
        # If we have time series, FCR usually trends. We want the "current" FCR?
        # Or average over period? Usually "Current FCR" is most important.
        # Let's take the average of the last values for each unit.
        if col_unit and col_date:
             latest_indices = df.groupby(col_unit)[col_date].idxmax().dropna()
             avg_fcr = df.loc[latest_indices, col_fcr].mean()
             kpis.append({
                 "label": "FCR (Actual)",
                 "value": f"{avg_fcr:.2f}",
                 "unit": ""
             })
        else:
             # Fallback
             avg_fcr = df[col_fcr].mean()
             kpis.append({
                 "label": "FCR Promedio",
                 "value": f"{avg_fcr:.2f}",
                 "unit": ""
             })

    # 4. Peso Promedio - Average of last
    col_wgt = get_col(['peso prom', 'avg weight', 'mean weight'])
    if col_wgt and col_unit and col_date:
        latest_indices = df.groupby(col_unit)[col_date].idxmax().dropna()
        avg_wgt = df.loc[latest_indices, col_wgt].mean()
        kpis.append({
            "label": "Peso Promedio (Actual)",
            "value": f"{avg_wgt:.3f}",
            "unit": "kg"
        })
    elif col_wgt:
        kpis.append({
            "label": "Peso Promedio",
            "value": f"{df[col_wgt].mean():.3f}",
            "unit": "kg"
        })

    # 5. Temperature (Ambiental) - Simple Average over period
    col_temp = get_col(['temp', 'degree', 'grados'])
    if col_temp:
        avg_temp = df[col_temp].mean()
        kpis.append({
            "label": "Temp. Promedio",
            "value": f"{avg_temp:.1f}",
            "unit": "°C"
        })
        
    return kpis
