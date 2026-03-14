import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

def create_main_chart(df: pd.DataFrame, variables: list, batch_comparison_mode: str = 'Overlay', x_axis_mode: str = 'Date', chart_type: str = 'Líneas', hover_mode: str = 'x unified', sum_units: bool = False, avg_units: bool = False, align_first: bool = False, highlight_points: list = None, unite_variables: bool = False, independent_axes: bool = False, rename_map: dict = None, pie_view_mode: str = "parents", kpi_thresholds: dict = None, active_kpis: list = None, proyecciones_df=None, variable_ranges: dict = None):
    """
    Creates the main Plotly chart.
    
    Args:
        chart_type: 'Líneas', 'Barras', 'Área'
    """
    if df.empty or not variables:
        fig = go.Figure()
        return fig

    # Rename helper
    _rmap = rename_map or {}
    _vranges = variable_ranges or {}
    def _display_name(col):
        return _rmap.get(col, col)

    def _resolve_var_range(var_name, var_col_name):
        candidates = [var_name, var_col_name]
        for c in candidates:
            if c in _vranges:
                return _vranges[c]
        for c in candidates:
            if c is None:
                continue
            c_low = str(c).lower()
            for k, v in _vranges.items():
                if str(k).lower() == c_low:
                    return v
        return None

    def _apply_visual_range(series, var_name, var_col_name):
        vr = _resolve_var_range(var_name, var_col_name)
        if not vr:
            return series
        try:
            rmin, rmax = vr
            y_num = pd.to_numeric(series, errors='coerce')
            mask = (y_num >= float(rmin)) & (y_num <= float(rmax))
            return series.where(mask)
        except Exception:
            return series

    # Helper to resolve columns using substring matching (handles 'Final Fecha', 'Batch', etc.)
    def get_col(name):
        """Find column containing 'name' (case-insensitive substring match)."""
        name_lower = name.lower()
        # 1. Exact match (case-insensitive)
        for c in df.columns:
            if c.lower() == name_lower:
                return c
        # 2. Substring match (first column containing the keyword)
        for c in df.columns:
            if name_lower in c.lower():
                return c
        return None

    def _get_series_col(df):
        """Determine which column to use for series grouping."""
        cols = [c.lower() for c in df.columns]
        
        # 1. NEW: Prioritize Mediciones "Lugar"
        # If the dataframe has 'Lugar de muestreo', use it!
        # (Checking for 'lugar' or 'muestreo' substring might be safer)
        for col in df.columns:
            if 'lugar' in col.lower() and 'muestreo' in col.lower():
                return col
                
        # 2. Batch (Lote)
        if 'lote' in cols: return df.columns[cols.index('lote')]
        if 'batch' in cols: return df.columns[cols.index('batch')]
        
        # 3. Unit (Jaula/Unidad)
        if 'jaula' in cols: return df.columns[cols.index('jaula')] 
        if 'unidad' in cols: return df.columns[cols.index('unidad')]
        if 'unit' in cols: return df.columns[cols.index('unit')]
        
        return None

    # Determine X-Axis Column
    x_col = None
    
    # Weekly mode: if 'Semana' column exists, use it as x-axis
    is_weekly = 'Semana' in df.columns
    if is_weekly:
        x_col = 'Semana'
    elif x_axis_mode == 'Days':
        # Priority order for days columns
        for cand in ['final days since first input', 'primer ingreso', 'first input', 'days', 'dias']:
            x_col = get_col(cand)
            if x_col:
                break
        if not x_col:
            # Fallback to date
            for cand in ['final fecha', 'fecha', 'date']:
                x_col = get_col(cand)
                if x_col:
                    break
    else:
        # Date mode: prioritize 'final fecha'
        for cand in ['final fecha', 'fecha', 'date']:
            x_col = get_col(cand)
            if x_col:
                break

    if not x_col:
         return go.Figure()

    # Determine Days Column for Hover (Requested Feature)
    days_col = None
    days_candidates = ['final days since first input', 'primer ingreso', 'first input', 'dias', 'days']
    for cand in days_candidates:
        found = next((c for c in df.columns if cand in c.lower()), None)
        if found:
            days_col = found
            break

    # Determine Date Column for Hover
    hover_date_col = None
    for cand in ['final fecha', 'fecha', 'date']:
        found = get_col(cand)
        if found:
            hover_date_col = found
            break

    # Identify series grouping
    lote_col = get_col('batch') or get_col('lote')
    unit_col = get_col('unidad') or get_col('unit') or get_col('jaula')
    dept_col = None
    for c in df.columns:
        cl = c.lower()
        if 'departamento' in cl or 'depto' in cl or ('dep' in cl and 'area' not in cl):
            dept_col = c
            break
    
    # Detect Mediciones mode (has "Lugar de muestreo" WITH actual data)
    lugar_col = None
    for c in df.columns:
        if 'lugar' in c.lower() and 'muestreo' in c.lower():
            lugar_col = c
            break
    
    # Only activate mediciones mode if the column has actual non-null values
    is_mediciones = lugar_col is not None and df[lugar_col].notna().any()

    # Create groupings
    df = df.copy()
    if is_mediciones:
        df['SeriesName'] = df[lugar_col].astype(str)
    elif avg_units:
        # Average Units: Group by Batch Only (data is already averaged)
        if lote_col:
            df['SeriesName'] = df[lote_col].astype(str)
        else:
            df['SeriesName'] = "Promedio Global"
    elif sum_units:
        # Units aggregated: group by Batch + Dept only
        if lote_col and dept_col:
            df['SeriesName'] = df[lote_col].astype(str) + " - " + df[dept_col].astype(str)
        elif lote_col:
            df['SeriesName'] = df[lote_col].astype(str)
        else:
            df['SeriesName'] = "Total"
    elif unit_col and lote_col and dept_col:
        df['SeriesName'] = df[lote_col].astype(str) + " - " + df[dept_col].astype(str) + " - " + df[unit_col].astype(str)
    elif unit_col and lote_col:
        df['SeriesName'] = df[lote_col].astype(str) + " - " + df[unit_col].astype(str)
    elif lote_col:
        df['SeriesName'] = df[lote_col].astype(str)
    else:
        df['SeriesName'] = "Total"

    unique_series = df['SeriesName'].unique()
    unique_batches = list(df[lote_col].unique()) if lote_col else []

    # Logic for Subplots
    is_subplots = (len(unique_batches) > 1 and batch_comparison_mode == 'Side-by-Side')
    
    # Mediciones subplots: one per variable when multiple places + line modes
    med_subplots = False
    if is_mediciones and len(unique_series) > 1 and len(variables) > 1 and chart_type in ['Líneas', 'Líneas + Marcadores', 'Área'] and not unite_variables:
        med_subplots = True
    
    # Standard subplots: one per variable when >1 variable (unless user chose to unite)
    std_var_subplots = False
    if not is_mediciones and len(variables) > 1 and not unite_variables:
        std_var_subplots = True
        
    
    # === Global Color Palettes ===
    # Vibrant palette for single-batch mode — maximally distinct on dark bg
    flat_colors = [
        '#5B9EF4', '#F4694C', '#4ECDC4', '#FFD93D', '#C678DD',
        '#FF8A5C', '#45E6B0', '#E06C9F', '#98D8C8', '#FF6B6B',
        '#61AFEF', '#E5C07B', '#56B6C2', '#BE5046', '#7EC8E3',
        '#FFAB76', '#88C999', '#D19FE8', '#F7DC6F', '#82E0AA',
        '#F1948A', '#85C1E9', '#F0B27A', '#A3E4D7',
    ]
    
    # Bold palette for unite-variables mode — each variable must pop
    var_colors = [
        '#61AFEF', '#E06C75', '#98C379', '#E5C07B', '#C678DD',
        '#56B6C2', '#FF8A5C', '#45E6B0', '#F7DC6F', '#BE5046',
    ]

    if chart_type == 'Torta':
        # --- PIE CHART LOGIC ---
        group_col = lote_col if lote_col else 'SeriesName'
        unique_groups = list(df[group_col].dropna().unique()) if group_col in df.columns else ['Total']
        
        # Denominator mapping to calculate 100% remainder
        cause_names_pie = [
            'Embrionaria', 'Deforme Embrionaria', 'Micosis', 'Daño Mecánico Otros',
            'Desadaptado', 'Deforme', 'Descompuesto', 'Aborto', 'Daño Mecánico',
            'Sin causa Aparente', 'Maduro', 'Muestras', 'Operculo Corto',
            'Rezagado', 'Nefrocalcinosis', 'Exofialosis', 'Daño Mecánico por Muestreo',
        ]
        DENOMINATOR_MAP = {
            "% Mortalidad Acumulada": "Poblacion Inicial",
            "% Pérdida Acumulada": "Poblacion Inicial",
            "% Eliminación Acumulada": "Poblacion Inicial",
            "% Mortalidad diaria": "Poblacion Diaria",
            "Pérdida diaria %": "Poblacion Diaria",
            "Eliminación diaria %": "Poblacion Diaria",
        }
        for c in cause_names_pie:
            DENOMINATOR_MAP[f"% Mortalidad {c} Acumulada"] = "Poblacion Inicial"
            DENOMINATOR_MAP[f"% Mortalidad {c} Diaria"] = "Poblacion Diaria"

        var_denominators = [DENOMINATOR_MAP.get(v) for v in variables]
        has_common_denom = len(set(var_denominators)) == 1 and var_denominators[0] is not None
        
        if len(variables) == 1 and not has_common_denom:
            var = variables[0]
            var_col = get_col(var)
            if not var_col: return go.Figure()
            
            # Aggregate sum per batch (lote) instead of series (jaula)
            agg_df = df.groupby(group_col)[var_col].sum().reset_index()
            
            labels = agg_df[group_col]
            values = agg_df[var_col]
            
            # In this case (e.g., Biomasa), the value IS the quantity
            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=labels,
                values=values,
                name=_display_name(var),
                hovertemplate="<b>%{label}</b><br>Cantidad: %{value:,.2f}<br>Porcentaje: %{percent}<extra></extra>",
                marker=dict(colors=flat_colors)
            ))
            fig.update_layout(title_text=f"Proporción de {_display_name(var)} por Lote")
            
        else:
            # Multiple Variables or 1 Variable WITH 100% Denominator
            if len(unique_groups) == 0:
                return go.Figure()
                
            fig = make_subplots(
                rows=1, cols=len(unique_groups),
                specs=[[{'type': 'domain'} for _ in range(len(unique_groups))]],
                subplot_titles=[f"Lote {g}" for g in unique_groups]
            )
            
            # --- Detect Hierarchy ---
            # Is 'Pérdida' selected alongside 'Mortalidad' and/or 'Eliminación'?
            var_lower_map = {v: v.lower() for v in variables}
            loss_vars = [v for v in variables if "pérdida" in var_lower_map[v]]
            mort_vars = [v for v in variables if "mortalidad" in var_lower_map[v] and "causa" not in var_lower_map[v]]
            elim_vars = [v for v in variables if "eliminación" in var_lower_map[v]]
            
            # Detect Trio logic dynamically based on selected vars (if the 3 original were selected, we know it's trio mode)
            # Actually, the variables passed here might already be replaced depending on the pie_view_mode.
            # But the requirement says: if % Eliminación Acumulada, % Mortalidad Acumulada and % Pérdida Acumulada are present
            # However, app.py passes different sets based on the view mode if trio is active. 
            # We can detect if this is a "Trio" by checking if all current vars belong to the extended trio set AND the original trio was selected.
            # A simpler way is: if there are ANY variables that sum up to 100% and need "last record" logic.
            is_trio_pie = any("eliminación acumulada" in v.strip().lower() for v in variables) and has_common_denom
            
            has_hierarchy = len(loss_vars) > 0 and (len(mort_vars) > 0 or len(elim_vars) > 0)
            
            for i, group_name in enumerate(unique_groups):
                group_df = df[df[group_col] == group_name] if group_col in df.columns else df
                
                # Dynamic common denominator extraction for the group
                denom_val_for_group = None
                if has_common_denom and var_denominators and var_denominators[0]:
                    denom_name = var_denominators[0]
                    if 'inicial' in denom_name.lower() and 'Cant inicial batch' in group_df.columns:
                        denom_val_for_group = pd.to_numeric(group_df['Cant inicial batch'], errors='coerce').max()
                    else:
                        denom_col = next((c for c in group_df.columns if denom_name.lower() in c.lower() or ('inicial' in denom_name.lower() and 'inicial' in c.lower())), None)
                        if not denom_col and 'inicial' in denom_name.lower():
                            denom_col = next((c for c in group_df.columns if 'población inicial' in c.lower()), None)
                        if denom_col:
                            if 'inicial' in denom_col.lower():
                                denom_s = pd.to_numeric(group_df[denom_col], errors='coerce')
                                denom_val_for_group = denom_s.max()
                            else:
                                denom_s = pd.to_numeric(group_df[denom_col], errors='coerce')
                                denom_val_for_group = denom_s.sum()
                
                labels = []
                values = []
                customdatas = []
                pie_colors = []
                
                # Helper to find the raw numerator column for a given percentage feature
                def _get_numerator_col_fallback(var_name):
                    v_lower = var_name.lower()
                    if 'mortalidad' in v_lower:
                        # Extract the exact cause name if present
                        cause = None
                        for c in cause_names_pie:
                            if c.lower() in v_lower:
                                cause = c.lower()
                                break
                        
                        if cause:
                            # Match the cause exactly in the column name along with mortal/numero/periodo
                            return next((c for c in group_df.columns if 'mortalidad' in c.lower() and 'número' in c.lower() and 'período' in c.lower() and cause in c.lower()), None)
                        else:
                            # General mortality (make sure it doesn't accidentally pick up a cause)
                            return next((c for c in group_df.columns if 'mortalidad' in c.lower() and 'número' in c.lower() and 'período' in c.lower() and not any(ca.lower() in c.lower() for ca in cause_names_pie)), None)
                    elif 'eliminación' in v_lower or 'eliminados' in v_lower:
                        return next((c for c in group_df.columns if 'eliminados' in c.lower() and 'número' in c.lower() and 'período' in c.lower()), None)
                    elif 'pérdida' in v_lower:
                        return next((c for c in group_df.columns if 'pérdida' in c.lower() and 'número' in c.lower() and 'período' in c.lower()), None)
                    return None
                
                # Calculate values for all selected variables
                var_vals = {}
                var_counts = {}
                
                # Sort group by date to get the last record
                if x_col in group_df.columns:
                    group_df = group_df.sort_values(by=x_col)
                
                for j, var in enumerate(variables):
                    var_col = get_col(var)
                    if var_col:
                        numeric_s = pd.to_numeric(group_df[var_col], errors='coerce')
                        
                        if is_trio_pie and 'acumulad' in var.lower():
                            # Trio logic: take the last valid record by date
                            valid_s = numeric_s.dropna()
                            total_val = valid_s.iloc[-1] if not valid_s.empty else 0
                        else:
                            # Standard logic
                            total_val = numeric_s.max() if 'acumulad' in var.lower() else numeric_s.sum()
                            
                        var_vals[var] = total_val if not pd.isna(total_val) else 0
                        
                        # Also calculate the actual numerator count if possible
                        num_col = _get_numerator_col_fallback(var)
                        if num_col:
                            num_s = pd.to_numeric(group_df[num_col], errors='coerce')
                            # For absolute counts we always sum the period values to get the total
                            var_counts[var] = num_s.sum() if not pd.isna(num_s.sum()) else 0
                        else:
                            # If we can't find a numerator, fallback to the value
                            var_counts[var] = var_vals[var]
                
                if has_hierarchy:
                    loss_var = loss_vars[0]
                    loss_val = var_vals.get(loss_var, 0)
                    loss_count = var_counts.get(loss_var, 0)
                    
                    unrelated = [v for v in variables if v not in loss_vars and v not in mort_vars and v not in elim_vars]
                    unrelated_sum = sum(var_vals.get(uv, 0) for uv in unrelated)
                    unrelated_count_sum = sum(var_counts.get(uv, 0) for uv in unrelated)
                    
                    if pie_view_mode == "parents":
                        # 1. Add "Pérdida"
                        labels.append(_display_name(loss_var))
                        values.append(loss_val)
                        customdatas.append(loss_count)
                        pie_colors.append(var_colors[0])
                        
                        # 2. Add Unrelated variables
                        c_idx = 1
                        for uv in unrelated:
                            labels.append(_display_name(uv))
                            values.append(var_vals.get(uv, 0))
                            customdatas.append(var_counts.get(uv, 0))
                            pie_colors.append(var_colors[c_idx % len(var_colors)])
                            c_idx += 1
                            
                        # 3. Add "Resto"
                        if has_common_denom:
                            root_sum = loss_val + unrelated_sum
                            resto = max(0, 100 - root_sum)
                            if resto > 0:
                                labels.append("Resto (Otros)")
                                values.append(resto)
                                # Multiply the raw proportion by the common denominator
                                resto_count = (resto / 100.0) * denom_val_for_group if denom_val_for_group else resto
                                customdatas.append(max(0, resto_count) if pd.notna(resto_count) else resto)
                                pie_colors.append('#2D3748')
                                
                    else: # pie_view_mode == "children"
                        c_idx = 1
                        # 1. Add children of "Pérdida"
                        for mv in mort_vars:
                            val = var_vals.get(mv, 0)
                            labels.append(_display_name(mv))
                            values.append(val)
                            customdatas.append(var_counts.get(mv, 0))
                            pie_colors.append(var_colors[c_idx % len(var_colors)])
                            c_idx += 1
                            
                        for ev in elim_vars:
                            val = var_vals.get(ev, 0)
                            labels.append(_display_name(ev))
                            values.append(val)
                            customdatas.append(var_counts.get(ev, 0))
                            pie_colors.append(var_colors[c_idx % len(var_colors)])
                            c_idx += 1
                            
                        # 2. Add Unrelated
                        for uv in unrelated:
                            labels.append(_display_name(uv))
                            values.append(var_vals.get(uv, 0))
                            customdatas.append(var_counts.get(uv, 0))
                            pie_colors.append(var_colors[c_idx % len(var_colors)])
                            c_idx += 1
                            
                        # 3. Add "Resto" (using the same baseline as parents!)
                        if has_common_denom:
                            root_sum = loss_val + unrelated_sum
                            resto = max(0, 100 - root_sum)
                            if resto > 0:
                                labels.append("Resto (Otros)")
                                values.append(resto)
                                resto_count = (resto / 100.0) * denom_val_for_group if denom_val_for_group else resto
                                customdatas.append(max(0, resto_count) if pd.notna(resto_count) else resto)
                                pie_colors.append('#2D3748')
                            
                else:
                    # Flat Pie
                    c_idx = 0
                    for var in variables:
                        labels.append(_display_name(var))
                        values.append(var_vals.get(var, 0))
                        c_cnt = var_counts.get(var, 0)
                        customdatas.append(c_cnt)
                        pie_colors.append(var_colors[c_idx % len(var_colors)])
                        c_idx += 1
                        
                    if has_common_denom:
                        suma = sum(var_vals.values())
                        resto = max(0, 100 - suma)
                        if resto > 0:
                            labels.append("Cantidad actual")
                            values.append(resto)
                            resto_count = (resto / 100.0) * denom_val_for_group if denom_val_for_group else resto
                            customdatas.append(max(0, resto_count) if pd.notna(resto_count) else resto)
                            pie_colors.append('#2D3748')
                
                # We use standard Pie, sorting disabled to keep Resto at the end
                fig.add_trace(go.Pie(
                    labels=labels,
                    values=values,
                    customdata=customdatas,
                    name=str(group_name),
                    hovertemplate="<b>%{label}</b><br>Cantidad: %{customdata:,.0f}<br>Porcentaje: %{percent}<extra></extra>",
                    marker=dict(colors=pie_colors),
                    sort=True,                 # <--- Enable sorting (will show largest to smallest in pie)
                    direction='clockwise',
                    textposition='inside',     # <--- Only show text inside (auto-hides if it doesn't fit)
                    textinfo='percent'
                ), row=1, col=i+1)
                
            title_txt = "Distribución por Lote"
            fig.update_layout(title_text=title_txt)
            
        # Para mejorar la visibilidad en la leyenda ya que Plotly no tiene tooltips nativos 
        # exclusivos para la leyenda independientes del gráfico, modificamos los labels
        # para que incluyan el porcentaje y/o valor si es un único gráfico de pie.
        if len(unique_groups) == 1 and values:
            total_sum = sum(values)
            if total_sum > 0:
                new_labels = []
                for label, val, c_data in zip(labels, values, customdatas):
                    pct = (val / total_sum) * 100
                    new_labels.append(f"{label} ({pct:.1f}%)")
                
                # Actualizar los labels en la traza
                fig.data[0].labels = new_labels
                # Redefinir la plantilla de hover
                fig.data[0].hovertemplate = "<b>%{label}</b><br>Cantidad: %{customdata:,.0f}<extra></extra>"

        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#A0AEC0'),
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(
                traceorder='normal',
                itemclick="toggle",
                itemdoubleclick="toggleothers"
            )
        )
        return fig
    
    # --- NON-PIE CHART LOGIC (Line, Bar, Area, Scatter) ---
    
    if med_subplots:
        fig = make_subplots(rows=len(variables), cols=1, subplot_titles=[_display_name(v) for v in variables], shared_xaxes=True, vertical_spacing=0.08)
    elif std_var_subplots:
        fig = make_subplots(rows=len(variables), cols=1, subplot_titles=[_display_name(v) for v in variables], shared_xaxes=True, vertical_spacing=0.06)
    elif is_subplots:
        fig = make_subplots(rows=1, cols=len(unique_batches), subplot_titles=[str(b) for b in unique_batches])
    else:
        fig = go.Figure()

    # === HIERARCHICAL COLOR SYSTEM ===
    # Color families: one hue per batch (HSL based)
    # Each family goes from light to dark as units increase
    import colorsys
    
    # 8 maximally distinct hue families for batch differentiation on dark bg
    # High saturation + good lightness = always visible on #0F1117
    batch_hues = [
        (220, 80, 60),  # Vivid Blue
        (10, 85, 60),   # Vivid Red-Orange
        (145, 75, 55),  # Emerald Green
        (40, 90, 58),   # Bright Amber
        (280, 70, 62),  # Vivid Purple
        (175, 80, 52),  # Cyan-Teal
        (340, 80, 62),  # Hot Pink
        (55, 85, 55),   # Lime-Yellow
    ]
    
    def hsl_to_hex(h, s, l):
        """Convert HSL (h=0-360, s=0-100, l=0-100) to hex color."""
        r, g, b = colorsys.hls_to_rgb(h/360, l/100, s/100)
        return f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'

    def hex_to_hsl(hex_color):
        """Convert hex to HSL (h=0-360, s=0-100, l=0-100)."""
        hex_color = hex_color.lstrip('#')
        r = int(hex_color[0:2], 16) / 255.0
        g = int(hex_color[2:4], 16) / 255.0
        b = int(hex_color[4:6], 16) / 255.0
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        return h * 360, s * 100, l * 100
    
    def get_batch_colors(batch_idx, num_units):
        """Generate a color gradient for units within a batch family."""
        if batch_idx < len(batch_hues):
            base_h, base_s, base_l = batch_hues[batch_idx]
        else:
            # Extra batches: distribute hue evenly in remaining space
            base_h = (batch_idx * 47 + 15) % 360
            base_s, base_l = 75, 58
        
        if num_units == 1:
            return [hsl_to_hex(base_h, base_s, base_l)]
        
        # Lightness: 75 (light, pastel-ish) → 45 (medium-dark, still visible on dark bg)
        colors_list = []
        for i in range(num_units):
            t = i / max(num_units - 1, 1)
            l = 75 - t * 30   # Range: 75 → 45 (never too dark to see)
            s = base_s + t * 8  # Slightly more saturated as darker
            colors_list.append(hsl_to_hex(base_h, min(s, 100), l))
        return colors_list
    
    # Build batch→series mapping and color assignments
    batch_series_map = {}  # {batch: [series_name1, series_name2, ...]}
    series_color_map = {}  # {series_name: color}
    
    if lote_col and len(unique_batches) > 1:
        # Multi-batch: hierarchical colors
        for b in unique_batches:
            batch_mask = df[lote_col] == b
            batch_series = df.loc[batch_mask, 'SeriesName'].unique()
            batch_series_map[b] = sorted(batch_series)
        
        for b_idx, batch in enumerate(unique_batches):
            series_in_batch = batch_series_map[batch]
            batch_colors = get_batch_colors(b_idx, len(series_in_batch))
            for s_idx, sn in enumerate(series_in_batch):
                series_color_map[sn] = batch_colors[s_idx]
    elif lote_col and len(unique_batches) == 1:
        # Single-batch: use full flat palette per unit
        sorted_series = sorted(unique_series)
        for idx, sn in enumerate(sorted_series):
            series_color_map[sn] = flat_colors[idx % len(flat_colors)]
    else:
        # No batch column: flat palette
        for idx, sn in enumerate(unique_series):
            series_color_map[sn] = flat_colors[idx % len(flat_colors)]
    
    # Dash styles per place
    dash_styles = ['solid', 'dash', 'dot', 'dashdot', 'longdash', 'longdashdot']
    
    # Marker symbols per place
    marker_symbols = ['circle', 'square', 'diamond', 'cross', 'x', 'triangle-up', 
                      'triangle-down', 'star', 'hexagon', 'pentagon']
    
    # Keyword list for Y2
    y2_keywords = ['%', 'percent', 'fcr', 'sfr', 'temp', 'grados', 'degree']

    # Detect Horario column
    horario_col = None
    for c in df.columns:
        if 'horario' in c.lower():
            horario_col = c
            break

    trace_idx = 0
    
    if is_mediciones:
        # === MEDICIONES MODE ===
        # Subplots (multi-place + multi-var): Color = Place (solid lines, variables separated)
        # Single plot: Color = Variable, Dash/Symbol = Place
        for j, var in enumerate(variables):
            var_col = get_col(var)
            if not var_col: continue
            
            subplot_row = j + 1 if med_subplots else None
            
            for p, place in enumerate(unique_series):
                place_data = df[df['SeriesName'] == place].sort_values(x_col)
                place_data = place_data.dropna(subset=[var_col])
                if place_data.empty: continue
                
                # Color strategy depends on layout
                if med_subplots:
                    trace_color = flat_colors[p % len(flat_colors)]
                else:
                    # Base hue for the variable
                    base_h, base_s, base_l = hex_to_hsl(var_colors[j % len(var_colors)])
                    num_places = len(unique_series)
                    if num_places > 1:
                        # Vary lightness to distinguish places for the same variable
                        t = p / (num_places - 1)
                        l = 80 - t * 35
                        s = min(base_s + t * 10, 100)
                        trace_color = hsl_to_hex(base_h, s, l)
                    else:
                        trace_color = hsl_to_hex(base_h, base_s, base_l)
                
                # User requested all lines solid ("sin punteo")
                place_dash = 'solid'
                
                place_symbol = marker_symbols[p % len(marker_symbols)]
            
                
                # Custom data for hover
                custom_data = None
                hover_extra = ""
                
                # Check for Horario column
                has_horario = False
                for c in place_data.columns:
                    if 'horario' in c.lower():
                        has_horario = True
                        break
                        
                # Extract sheet_name if present
                has_sheet = 'sheet_name' in place_data.columns
                sheet_extra = ""
                
                # Build unified customdata array
                cd_cols = []
                cd_map = {}
                
                if days_col and days_col in place_data.columns:
                    cd_cols.append(place_data[days_col].values)
                    cd_map['days'] = len(cd_cols) - 1
                
                if has_horario:
                    cd_cols.append(place_data[horario_col].values)
                    cd_map['horario'] = len(cd_cols) - 1
                    hover_extra += f"Horario: %{{customdata[{cd_map['horario']}]}}<br>"
                    
                if has_sheet:
                    cd_cols.append(place_data['sheet_name'].values)
                    cd_map['sheet'] = len(cd_cols) - 1
                    hover_extra += f"%{{customdata[{cd_map['sheet']}]}}<br>"
                
                if cd_cols:
                    import numpy as np
                    custom_data = np.column_stack(cd_cols)
                
                # Retrieve the actual sheet name for the legend (assuming uniform per place/trace)
                sheet_str = place_data['sheet_name'].iloc[0] if has_sheet and not place_data.empty else ""
                
                # Hover template
                if hover_mode == 'closest':
                    ht = f"<b>{var}</b><br>"
                    ht += f"Fecha: %{{x|%d-%m-%Y}}<br>"
                    ht += hover_extra
                    if str(place).strip() != 'General':
                        ht += f"Lugar: {place}<br>"
                    ht += f"Valor: %{{y}}"
                    ht += "<extra></extra>"
                else:
                    ht = f"%{{y}}<br>{hover_extra}<extra></extra>"
                
                # Legend and trace name (Remove "General" if present)
                place_str = str(place).strip()
                trace_suffix = f"({sheet_str})" if sheet_str else ""
                
                if place_str == 'General':
                    trace_name = f"{var} {trace_suffix}"
                    legend_grp = f"{var} {trace_suffix}"
                else:
                    trace_name = f"{place} - {var} {trace_suffix}" if not med_subplots else f"{place} {trace_suffix}"
                    legend_grp = f"{place} {trace_suffix}" if med_subplots else trace_name
                
                trace_params = dict(
                    x=place_data[x_col],
                    y=_apply_visual_range(place_data[var_col], var, var_col),
                    name=trace_name.strip(),
                    customdata=custom_data,
                    hovertemplate=ht,
                    legendgroup=legend_grp.strip() if isinstance(legend_grp, str) else legend_grp,
                    showlegend=(j == 0) if med_subplots else True,
                )
                
                # Independent Axes setup
                if unite_variables and independent_axes and len(variables) > 1:
                    trace_params['yaxis'] = f'y{j + 1}' if j > 0 else 'y'

                
                if chart_type == 'Barras':
                    trace = go.Bar(**trace_params, marker=dict(color=trace_color, opacity=0.8))
                elif chart_type == 'Dispersión':
                    trace = go.Scatter(
                        **trace_params,
                        mode='markers',
                        marker=dict(size=12, color=trace_color, opacity=0.85, symbol=place_symbol,
                                    line=dict(width=1, color='#1A1D24'))
                    )
                elif chart_type == 'Área':
                    trace = go.Scatter(
                        **trace_params,
                        mode='lines',
                        fill='tozeroy' if p == 0 else 'tonexty',
                        connectgaps=True,
                        line=dict(color=trace_color, width=2, dash=place_dash, shape='spline')
                    )
                elif chart_type == 'Líneas + Marcadores':
                    trace = go.Scatter(
                        **trace_params,
                        mode='lines+markers',
                        connectgaps=True,
                        line=dict(color=trace_color, width=3, dash=place_dash, shape='spline'),
                        marker=dict(size=10, color=trace_color, symbol=place_symbol, opacity=0.9,
                                    line=dict(width=1, color='#1A1D24'))
                    )
                else:  # Líneas
                    trace = go.Scatter(
                        **trace_params,
                        mode='lines',
                        connectgaps=True,
                        line=dict(color=trace_color, width=3, dash=place_dash, shape='spline')
                    )
                
                if med_subplots:
                    fig.add_trace(trace, row=subplot_row, col=1)
                else:
                    fig.add_trace(trace)
                trace_idx += 1
    else:
        # === STANDARD MODE (Main Chart) ===
        # Color = hierarchical by batch/unit, subplots by variable
        
        # === SPECIAL: Stacked bar chart for mortality/loss/elimination trio ===
        vars_lower = [v.strip().lower() for v in variables]
        
        # Detect daily trio
        daily_trio = {'% mortalidad diaria', 'pérdida diaria %', 'eliminación diaria %'}
        has_daily_trio = daily_trio.issubset(set(vars_lower))
        
        # Detect accumulated trio
        acum_trio = {'% mortalidad acumulada', '% pérdida acumulada', '% eliminación acumulada'}
        has_acum_trio = acum_trio.issubset(set(vars_lower))
        
        if (has_daily_trio or has_acum_trio) and lote_col and unite_variables:
            # Determine which trio
            if has_daily_trio:
                mort_var = next(v for v in variables if v.strip().lower() == '% mortalidad diaria')
                elim_var = next(v for v in variables if v.strip().lower() == 'eliminación diaria %')
                perd_var = next(v for v in variables if v.strip().lower() == 'pérdida diaria %')
                trio_label = "diaria"
            else:
                mort_var = next(v for v in variables if v.strip().lower() == '% mortalidad acumulada')
                elim_var = next(v for v in variables if v.strip().lower() == '% eliminación acumulada')
                perd_var = next(v for v in variables if v.strip().lower() == '% pérdida acumulada')
                trio_label = "acumulada"
            
            mort_col = get_col(mort_var)
            elim_col = get_col(elim_var)
            
            # Render stacked bars per batch
            bar_colors_mort = ['#EF5350', '#E57373', '#EF9A9A', '#FFCDD2', '#F44336', '#D32F2F']
            bar_colors_elim = ['#FFA726', '#FFB74D', '#FFCC80', '#FFE0B2', '#FF9800', '#F57C00']
            
            for bi, batch_name in enumerate(unique_batches):
                batch_data = df[df[lote_col] == batch_name].copy()
                if mort_col:
                    batch_data = batch_data.dropna(subset=[mort_col])
                batch_data = batch_data.drop_duplicates(subset=[x_col]).sort_values(x_col)
                if batch_data.empty:
                    continue
                
                x_data = batch_data[x_col]
                if align_first and x_axis_mode == 'Days':
                    x_data = x_data - x_data.min()
                
                mort_vals = pd.to_numeric(batch_data[mort_col], errors='coerce').fillna(0) if mort_col else 0
                elim_vals = pd.to_numeric(batch_data[elim_col], errors='coerce').fillna(0) if elim_col else 0
                
                batch_label = f"Lote {batch_name}"
                c_mort = bar_colors_mort[bi % len(bar_colors_mort)]
                c_elim = bar_colors_elim[bi % len(bar_colors_elim)]
                
                # Build hover template for bars
                if is_weekly and '_week_start_str' in batch_data.columns:
                    import numpy as np
                    bar_cd = np.column_stack([
                        batch_data['_week_start_str'].values.astype(object),
                        batch_data['_week_end_str'].values.astype(object)
                    ])
                    mort_ht = f"<b>{batch_label}</b><br>Semana: %{{x}}<br>Desde: %{{customdata[0]}}<br>Hasta: %{{customdata[1]}}<br>Mortalidad: %{{y:.2f}}%<extra></extra>"
                    elim_ht = f"<b>{batch_label}</b><br>Semana: %{{x}}<br>Desde: %{{customdata[0]}}<br>Hasta: %{{customdata[1]}}<br>Eliminación: %{{y:.2f}}%<extra></extra>"
                else:
                    bar_cd = None
                    mort_ht = f"<b>{batch_label}</b><br>Mortalidad: %{{y:.2f}}%<extra></extra>"
                    elim_ht = f"<b>{batch_label}</b><br>Eliminación: %{{y:.2f}}%<extra></extra>"
                
                # Mortalidad bar (bottom)
                fig.add_trace(go.Bar(
                    x=x_data, y=mort_vals,
                    name=f"{batch_label} - % Mortalidad",
                    marker_color=c_mort,
                    legendgroup=batch_label,
                    customdata=bar_cd,
                    hovertemplate=mort_ht
                ))
                
                # Eliminación bar (stacked on top)
                fig.add_trace(go.Bar(
                    x=x_data, y=elim_vals,
                    name=f"{batch_label} - % Eliminación",
                    marker_color=c_elim,
                    legendgroup=batch_label,
                    customdata=bar_cd,
                    hovertemplate=elim_ht
                ))
            
            fig.update_layout(barmode='stack')
            
            # Remove trio vars from variables so they don't render again as lines
            variables = [v for v in variables if v.strip().lower() not in (daily_trio | acum_trio)]
        
        for j, var in enumerate(variables):
            var_col = get_col(var)
            if not var_col: continue
            
            subplot_row = j + 1 if std_var_subplots else None
            
            _var_lower = var.strip().lower()
            is_fcr_acum = _var_lower in ('fcr económico acumulado', 'fcr economico acumulado', 'fcr biológico acumulado', 'fcr biologico acumulado', 'gf3 acumulado', 'sgr acumulado', 'sfr acumulado', '% mortalidad acumulada', '% mortalidad diaria', '% pérdida acumulada', 'pérdida diaria %', '% eliminación acumulada', 'eliminación diaria %', 'peso promedio') or (_var_lower.startswith('% mortalidad') and (_var_lower.endswith('diaria') or _var_lower.endswith('acumulada')))
            
            # Variables that should be summed across units per day per batch
            is_sum_var = _var_lower in ('final número', 'final numero', 'n°final')
            
            # If it's FCR Acumulado or sum-var, plot one line per batch instead of per series
            series_to_plot = unique_batches if ((is_fcr_acum or is_sum_var) and lote_col) else unique_series
            
            for i, series_name in enumerate(series_to_plot):
                batch_idx = None
                
                if is_sum_var and lote_col:
                    # Sum across units per day for this batch
                    batch_data = df[df[lote_col] == series_name].copy()
                    if var_col in batch_data.columns:
                        batch_data = batch_data.dropna(subset=[var_col])
                    # Group by x-axis column and sum, preserving hover columns
                    if x_col in batch_data.columns:
                        agg_dict = {var_col: 'sum'}
                        if days_col and days_col in batch_data.columns:
                            agg_dict[days_col] = 'first'
                        if hover_date_col and hover_date_col in batch_data.columns:
                            agg_dict[hover_date_col] = 'first'
                        if is_weekly and '_week_start_str' in batch_data.columns:
                            agg_dict['_week_start_str'] = 'first'
                            agg_dict['_week_end_str'] = 'first'
                            
                        series_data = batch_data.groupby(x_col, as_index=False).agg(agg_dict).sort_values(x_col)
                    else:
                        # Fallback: dedup like is_fcr_acum
                        series_data = batch_data.drop_duplicates(subset=[c for c in [x_col] if c in batch_data.columns] or batch_data.columns[:1]).sort_values(batch_data.columns[0])
                    
                    display_series_name = f"Lote {series_name}"
                    current_batch = str(series_name)
                    if series_name in unique_batches:
                        batch_idx = list(unique_batches).index(series_name)
                elif is_fcr_acum and lote_col:
                    # series_name is actually the batch name
                    batch_data = df[df[lote_col] == series_name].copy()
                    # Drop rows where the acumulado variable is NaN/null before dedup
                    if var_col in batch_data.columns:
                        batch_data = batch_data.dropna(subset=[var_col])
                    series_data = batch_data.drop_duplicates(subset=[x_col]).sort_values(x_col)
                    
                    display_series_name = f"Lote {series_name}"
                    current_batch = str(series_name)
                    # Find index of this batch for coloring
                    if series_name in unique_batches:
                        batch_idx = list(unique_batches).index(series_name)
                else:
                    series_data = df[df['SeriesName'] == series_name].sort_values(x_col)
                    display_series_name = series_name
                    current_batch = str(series_data[lote_col].iloc[0]) if lote_col else None
                    
                if series_data.empty: continue

                # Align at First Record Logic
                x_data = series_data[x_col]
                if align_first and x_axis_mode == 'Days':
                    # Subtract min value to start at 0
                    min_val = x_data.min()
                    x_data = x_data - min_val
                
                # Color assignment
                if unite_variables:
                    # United: Base Hue per Variable, Lightness/Shade per Series
                    base_h, base_s, base_l = hex_to_hsl(var_colors[j % len(var_colors)])
                    
                    num_series = len(series_to_plot)
                    if num_series > 1:
                        # Distribute lightness from 80 (light) to 45 (dark)
                        t = i / (num_series - 1)
                        l = 80 - t * 35
                        s = min(base_s + t * 10, 100) # Slightly more saturated as darker
                        color = hsl_to_hex(base_h, s, l)
                    else:
                        color = hsl_to_hex(base_h, base_s, base_l)
                elif (is_fcr_acum or is_sum_var) and lote_col and batch_idx is not None:
                    # Solid batch color, middle lightness to stand out
                    colors_list = get_batch_colors(batch_idx, 1)
                    color = colors_list[0] if colors_list else flat_colors[i % len(flat_colors)]
                else:
                    # Subplots: color = series (hierarchical batch color)
                    color = series_color_map.get(series_name, flat_colors[trace_idx % len(flat_colors)])
                
                # Format Y value
                y_fmt = ""
                if 'ventas' in var.lower() and 'biomasa' in var.lower():
                    y_fmt = ":,.0f"
                
                # Build rich customdata for hover
                # Columns: [days, var_name, series_name]
                import numpy as np
                cd_cols = []
                cd_map = {}
                
                if days_col and days_col in series_data.columns:
                    cd_cols.append(series_data[days_col].values.astype(object))
                    cd_map['days'] = len(cd_cols) - 1
                
                if hover_date_col and hover_date_col in series_data.columns:
                    # Robust formatting: convert to DD-MM-YYYY string in Python
                    try:
                        date_series = pd.to_datetime(series_data[hover_date_col], errors='coerce')
                        formatted = date_series.dt.strftime('%d-%m-%Y').fillna('').astype(object)
                        cd_cols.append(formatted.values)
                    except:
                        cd_cols.append(series_data[hover_date_col].astype(str).values.astype(object))
                    
                    cd_map['date'] = len(cd_cols) - 1
                
                # Add constant columns for var name and series name (force object dtype)
                cd_cols.append(np.full(len(series_data), var, dtype=object))
                cd_map['var'] = len(cd_cols) - 1
                cd_cols.append(np.full(len(series_data), display_series_name, dtype=object))
                cd_map['series'] = len(cd_cols) - 1
                
                # Weekly mode: add week start/end date strings
                if is_weekly and '_week_start_str' in series_data.columns and '_week_end_str' in series_data.columns:
                    cd_cols.append(series_data['_week_start_str'].values.astype(object))
                    cd_map['week_from'] = len(cd_cols) - 1
                    cd_cols.append(series_data['_week_end_str'].values.astype(object))
                    cd_map['week_to'] = len(cd_cols) - 1
                
                custom_data = np.column_stack(cd_cols) if cd_cols else None
                
                # Build rich hover template
                ht = f"<b>%{{customdata[{cd_map['var']}]}}</b><br>"
                
                if is_weekly and 'week_from' in cd_map:
                    ht += f"Semana: %{{x}}<br>"
                    ht += f"Desde: %{{customdata[{cd_map['week_from']}]}}<br>"
                    ht += f"Hasta: %{{customdata[{cd_map['week_to']}]}}<br>"
                elif x_axis_mode == 'Days' and 'date' in cd_map:
                    ht += f"Fecha: %{{customdata[{cd_map['date']}]}}<br>"
                else:
                    ht += f"Fecha: %{{x|%d-%m-%Y}}<br>"
                ht += f"Valor: %{{y{y_fmt}}}"
                if 'days' in cd_map:
                    ht += f"<br>Días Cultivo: %{{customdata[{cd_map['days']}]}}"
                ht += f"<extra>%{{customdata[{cd_map['series']}]}}</extra>"

                # Trace naming and legend
                if unite_variables:
                    # All vars on one chart: name includes variable
                    t_name = f"{display_series_name} - {var}"
                    t_legend = f"{display_series_name} - {var}"
                    t_showlegend = True
                elif is_fcr_acum or is_sum_var:
                    # Acumulado vars: unique legend group per variable+batch
                    # so multiple acumulado variables don't merge in the legend
                    t_name = f"{display_series_name} - {var}" if len(variables) > 1 else display_series_name
                    t_legend = f"{display_series_name} - {var}"
                    t_showlegend = True
                else:
                    # Subplots per variable: show series once across all subplots
                    t_name = display_series_name
                    t_legend = display_series_name
                    t_showlegend = (j == 0)

                trace_params = dict(
                    x=x_data,
                    y=_apply_visual_range(series_data[var_col], var, var_col),
                    name=t_name,
                    customdata=custom_data,
                    hovertemplate=ht,
                    legendgroup=t_legend,
                    showlegend=t_showlegend,
                )
                
                # Independent Axes setup
                if unite_variables and independent_axes and len(variables) > 1:
                    # j is the index of the variable
                    trace_params['yaxis'] = f'y{j + 1}' if j > 0 else 'y'

                if chart_type == 'Barras':
                    trace = go.Bar(**trace_params, marker=dict(color=color, opacity=0.8))
                elif chart_type == 'Área':
                    trace = go.Scatter(
                        **trace_params,
                        mode='lines', fill='tozeroy', connectgaps=True,
                        line=dict(color=color, width=2, shape='spline'),
                    )
                elif chart_type == 'Dispersión':
                    trace = go.Scatter(
                        **trace_params,
                        mode='markers',
                        marker=dict(size=10, color=color, opacity=0.85,
                                    line=dict(width=1, color='#1A1D24'))
                    )
                elif chart_type == 'Líneas + Marcadores':
                    trace = go.Scatter(
                        **trace_params,
                        mode='lines+markers', connectgaps=True,
                        line=dict(color=color, width=2, shape='spline'),
                        marker=dict(size=8, color=color, opacity=0.9,
                                    line=dict(width=1, color='#1A1D24'))
                    )
                else:  # Líneas
                    trace = go.Scatter(
                        **trace_params,
                        mode='lines', connectgaps=True,
                        line=dict(color=color, width=2, shape='spline')
                    )
                
                if std_var_subplots:
                    fig.add_trace(trace, row=subplot_row, col=1)
                elif is_subplots:
                    col_idx = unique_batches.index(current_batch) + 1 if current_batch in unique_batches else 1
                    fig.add_trace(trace, row=1, col=col_idx)
                else:
                    fig.add_trace(trace)
                trace_idx += 1
    
    # === Highlight Selected Points (Measurement Mode) ===
    if highlight_points:
        # Expected format: [{'x': val, 'y': val}, ...]
        
        # Sort points by X for consistent display logic (Start -> End)
        # Note: We sort a copy so we don't mutate the passed list if it matters
        sorted_points = []
        try:
            # Try to sort assuming X is comparable (numbers or date strings)
            # If dates, string comparison usually works for ISO/standard formats
            # If not, we fall back to index order
            sorted_points = sorted(highlight_points, key=lambda p: p['x'])
        except:
             sorted_points = list(highlight_points)

        for i, pt in enumerate(sorted_points):
            # Determine color based on sorted order (Start=Red, End=Green)
            color = '#F56565' if i == 0 else '#48BB78' 
            symbol = 'circle-open-dot'
            name = "Inicio" if i == 0 else "Fin"
            
            # Determine Axis Reference from curveNumber
            xref, yref = 'x', 'y'
            try:
                c_idx = pt.get('curveNumber', 0)
                # Ensure index is within range (config might have changed)
                if c_idx < len(fig.data):
                    trace = fig.data[c_idx]
                    xref = trace.xaxis if trace.xaxis else 'x'
                    yref = trace.yaxis if trace.yaxis else 'y'
            except:
                pass

            # 1. Markers (Attached to specific axis)
            fig.add_trace(go.Scatter(
                x=[pt['x']], y=[pt['y']],
                mode='markers',
                marker=dict(size=14, color=color, symbol=symbol, line=dict(width=3, color=color)),
                name=name,
                xaxis=xref, 
                yaxis=yref,
                showlegend=False,
                hoverinfo='skip'
            ))
            
            # 2. Crosshairs (Infinite Lines constrained to Subplot)
            # Use 'domain' reference for the spanning dimension
            y_domain_ref = f"{yref} domain"
            x_domain_ref = f"{xref} domain"
            
            # Vertical (Span Y domain, fixed at X data)
            fig.add_shape(
                type="line",
                x0=pt['x'], x1=pt['x'], 
                y0=0, y1=1,
                xref=xref, 
                yref=y_domain_ref,
                line=dict(color=color, width=1, dash="dot"),
                layer="below",
                opacity=0.7
            )
            # Horizontal (Span X domain, fixed at Y data)
            fig.add_shape(
                type="line",
                x0=0, x1=1, 
                y0=pt['y'], y1=pt['y'],
                xref=x_domain_ref, 
                yref=yref,
                line=dict(color=color, width=1, dash="dot"),
                layer="below",
                opacity=0.7
            )

        if len(sorted_points) == 2:
            p1, p2 = sorted_points[0], sorted_points[1]
            
            # 3. Selection Area (Rectangle)
            # We draw a rect from (x1, y1) to (x2, y2)
            # Use semi-transparent fill
            # Must attach to relevant AXIS. If points are on different axes, this is tricky.
            # We assume user clicks on the SAME chart usually.
            # If different, we pick the axis of the first point or maybe 'xref' if available.
            # Let's fallback to 'x'/'y' if mixed, or just use p1's axis.
            
            rect_xref, rect_yref = 'x', 'y'
            try:
                # Use p1 axis references
                c_idx1 = p1.get('curveNumber', 0)
                if c_idx1 < len(fig.data):
                     t1 = fig.data[c_idx1]
                     rect_xref = t1.xaxis if t1.xaxis else 'x'
                     rect_yref = t1.yaxis if t1.yaxis else 'y'
            except:
                pass

            fig.add_shape(
                type="rect",
                x0=p1['x'], y0=p1['y'],
                x1=p2['x'], y1=p2['y'],
                xref=rect_xref,
                yref=rect_yref,
                line=dict(width=0),
                fillcolor="rgba(255, 255, 255, 0.08)", # Very subtle light fill
                layer="below"
            )
            
            # 4. Floating Delta Card (Annotation)
            try:
                y1 = p1['y']
                y2 = p2['y']
                dy = y2 - y1
                pct = (dy / y1 * 100) if y1 != 0 else 0
                
                # Format
                dy_fmt = f"{dy:+,.2f}"
                pct_fmt = f"{pct:+.1f}%"
                
                # Text Content with HTML styling
                card_text = (
                    f"<span style='font-size:1.1em; font-weight:bold; color:#FAFAFA'>ΔY: {dy_fmt}</span> "
                    f"<span style='color:{'#48BB78' if dy >=0 else '#F56565'}'>({pct_fmt})</span><br>"
                    f"<span style='font-size:0.85em; color:#A0AEC0'>{p1['x']} ➝ {p2['x']}</span>"
                )
                
                fig.add_annotation(
                    x=0.5, y=0.98, xref="paper", yref="paper",
                    text=card_text,
                    showarrow=False,
                    align="center",
                    bgcolor="rgba(26, 33, 44, 0.95)",
                    bordercolor="rgba(255, 255, 255, 0.15)",
                    borderwidth=1,
                    borderpad=10,
                    font=dict(family="Inter, sans-serif", size=13),
                    width=220
                )
            except:
                pass

    # Layout Updates
    # Legend: vertical on right for standard (many series), horizontal for mediciones
    if is_mediciones:
        legend_cfg = dict(
            orientation="h", y=-0.1, x=0.5, xanchor="center",
            bgcolor="rgba(0,0,0,0)", font=dict(color="#FAFAFA")
        )
    else:
        legend_cfg = dict(
            orientation="v", y=1, x=1.02, xanchor="left", yanchor="top",
            bgcolor="rgba(0,0,0,0)", font=dict(color="#FAFAFA", size=10),
            tracegroupgap=2
        )
    
    # Base Layout configuration
    layout_args = dict(
        template="plotly_dark",
        paper_bgcolor="#1A1D24",
        plot_bgcolor="#0F1117",
        font=dict(
            family="Inter, sans-serif",
            size=12,
            color="#A0AEC0"
        ),
        legend=legend_cfg,
        hovermode=hover_mode,
        clickmode='event+select',
        hoverdistance=10, # Standard distance
        spikedistance=1000, # Show spike from far away if needed
        hoverlabel=dict(
            bgcolor="rgba(30, 33, 40, 0.92)",
            bordercolor="rgba(255,255,255,0.15)",
            font=dict(family="Inter, sans-serif", size=13, color="#FAFAFA"),
            namelength=-1,  # Show full series name in colored box
        ),
        margin=dict(l=20, r=150 if not is_mediciones else 20, t=60, b=20),
        height=max(400 * len(variables), 500) if (med_subplots or std_var_subplots) else 600,
    )
    
    # === Configure Multiple Y-Axes for United Variables Mode ===
    if unite_variables and independent_axes and len(variables) > 1:
        # We need to map y1, y2, y3...
        # y1 is the main yaxis on the left.
        # Additional axes will be placed alternately on right and left, with offsets
        num_extra_axes = len(variables) - 1
        
        left_axes_count = 1  # y1 is always left
        right_axes_count = 0
        for i in range(1, len(variables)):
            if i % 2 == 1:
                right_axes_count += 1
            else:
                left_axes_count += 1
                
        # Each offset axis takes ~0.08 domain (8%)
        domain_start = 0.0 + (max(0, left_axes_count - 1) * 0.08)
        domain_end   = 1.0 - (max(0, right_axes_count - 1) * 0.08)
        
        layout_args['xaxis'] = dict(domain=[domain_start, domain_end])
        
        # Setup yaxis (y1) title and color to match its first trace
        base_h, base_s, base_l = hex_to_hsl(var_colors[0])
        color1 = hsl_to_hex(base_h, base_s, base_l)
        
        layout_args['yaxis'] = dict(
            title=dict(text=variables[0], font=dict(color=color1, size=11)),
            tickfont=dict(color=color1, size=10)
        )
        
        left_offset_idx = 0
        right_offset_idx = 0
        
        for i in range(1, len(variables)):
            axis_name = f'yaxis{i+1}'
            base_h, base_s, base_l = hex_to_hsl(var_colors[i % len(var_colors)])
            ax_color = hsl_to_hex(base_h, base_s, base_l)
            
            is_right = (i % 2 == 1)
            position = 0
            if is_right:
                if right_offset_idx == 0:
                    position = domain_end
                else:
                    position = domain_end + (right_offset_idx * 0.08)
                right_offset_idx += 1
            else:
                left_offset_idx += 1
                position = domain_start - (left_offset_idx * 0.08)
            
            layout_args[axis_name] = dict(
                title=dict(text=variables[i], font=dict(color=ax_color, size=11)),
                tickfont=dict(color=ax_color, size=10),
                overlaying='y',
                side='right' if is_right else 'left',
                position=position,
                showgrid=False,
                zeroline=False
            )
            
    fig.update_layout(**layout_args)
    
    any_subplots = is_subplots or med_subplots or std_var_subplots
    
    if not any_subplots:
        # Single plot layout (maybe dual axis)
        layout_args['yaxis'] = dict(
            title=variables[0] if variables else "%", 
            showgrid=True, 
            gridcolor="#2B303B"
        )
            
        fig.update_layout(**layout_args)
        x_title = 'Semana' if is_weekly else x_axis_mode
        fig.update_xaxes(title=dict(text=x_title, standoff=0), showgrid=True, gridcolor="#2B303B",
                         showspikes=True, spikemode='across', spikethickness=1,
                         spikecolor='#4A5568', spikedash='dot',
                         dtick=1 if is_weekly else None)
        
    else:
        # Subplots layout
        fig.update_layout(**layout_args)
        fig.update_xaxes(title=dict(text=x_axis_mode, standoff=0), showgrid=True, gridcolor="#2B303B",
                         showspikes=True, spikemode='across', spikethickness=1,
                         spikecolor='#4A5568', spikedash='dot')
        fig.update_yaxes(showgrid=True, gridcolor="#2B303B")

    # === KPI Threshold Lines ===
    if kpi_thresholds and active_kpis and chart_type != 'Torta':
        import unicodedata as _ud
        def _kpi_norm(s):
            s = _ud.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
            return s.strip().lower()

        kpi_colors = {
            'Hatchery': '#FF6B6B',
            'Fry': '#FFA726',
            'Alevinaje': '#FFEE58',
            'Smolt 1': '#66BB6A',
            'Smolt 2': '#42A5F5',
        }
        default_kpi_color = '#EF5350'

        # Resolve dept and date columns from the production df
        _dept_col = get_col('departamento') or get_col('depto') or get_col('dept')
        _date_col = get_col('fecha') or get_col('date')

        # Build dept → (min_date, max_date) lookup from the actual data
        dept_date_ranges = {}
        if _dept_col and _date_col:
            for dept_name in df[_dept_col].dropna().unique():
                dept_mask = df[_dept_col] == dept_name
                dept_dates = pd.to_datetime(df.loc[dept_mask, _date_col], errors='coerce').dropna()
                if not dept_dates.empty:
                    dept_date_ranges[str(dept_name).strip()] = (dept_dates.min(), dept_dates.max())

        for kpi_tipo in active_kpis:
            dept_vals = kpi_thresholds.get(kpi_tipo, {})
            if not dept_vals:
                continue

            # Build sorted list of segments by start date
            segments = []
            for dept, threshold_val in dept_vals.items():
                date_range = dept_date_ranges.get(dept)
                if not date_range:
                    continue
                segments.append({
                    'dept': dept,
                    'val': threshold_val * 100,
                    'x0': date_range[0],
                    'x1': date_range[1],
                    'color': kpi_colors.get(dept, default_kpi_color),
                })

            if not segments:
                continue

            # Sort by start date
            segments.sort(key=lambda s: s['x0'])

            # Check if all departments share the same threshold value
            unique_vals = set(round(s['val'], 6) for s in segments)
            all_same = len(unique_vals) == 1

            if all_same:
                # Single merged line across the full date range
                merged_val = segments[0]['val']
                x_start = segments[0]['x0']
                x_end = segments[-1]['x1']
                fig.add_shape(
                    type='line',
                    x0=x_start, x1=x_end,
                    y0=merged_val, y1=merged_val,
                    line=dict(color=default_kpi_color, width=1.5, dash='dot'),
                    opacity=0.8,
                    xref='x', yref='y',
                )
                fig.add_annotation(
                    x=x_end, y=merged_val,
                    text=f"{kpi_tipo}: {merged_val:.2f}%",
                    showarrow=False,
                    font=dict(size=10, color=default_kpi_color),
                    bgcolor="rgba(26, 29, 36, 0.7)",
                    xanchor='left',
                    yanchor='bottom',
                    xshift=5,
                )
            else:
                # Step-function with per-department segments + vertical connectors
                for i, seg in enumerate(segments):
                    h_start = seg['x0'] if i == 0 else segments[i - 1]['x1']

                    fig.add_shape(
                        type='line',
                        x0=h_start, x1=seg['x1'],
                        y0=seg['val'], y1=seg['val'],
                        line=dict(color=seg['color'], width=1.5, dash='dot'),
                        opacity=0.8,
                        xref='x', yref='y',
                    )
                    fig.add_annotation(
                        x=seg['x1'], y=seg['val'],
                        text=f"KPI {seg['dept']}: {seg['val']:.2f}%",
                        showarrow=False,
                        font=dict(size=10, color=seg['color']),
                        bgcolor="rgba(26, 29, 36, 0.7)",
                        xanchor='left',
                        yanchor='bottom',
                        xshift=5,
                    )

                    if i > 0:
                        prev = segments[i - 1]
                        fig.add_shape(
                            type='line',
                            x0=prev['x1'], x1=prev['x1'],
                            y0=prev['val'], y1=seg['val'],
                            line=dict(color='#4A5568', width=1, dash='dot'),
                            opacity=0.6,
                            xref='x', yref='y',
                        )



    # === Projection Overlay Traces ===
    if proyecciones_df is not None and not proyecciones_df.empty and chart_type != 'Torta':
        proj_df = proyecciones_df.copy()
        proj_fecha_col = next((c for c in proj_df.columns if 'fecha' in c.lower() or 'date' in c.lower()), None)
        proj_batch_col = 'batch' if 'batch' in proj_df.columns else None

        PROJ_PAIR_MAP = {
            'SGR Plan': 'SGR Acumulado',
            'SFR Plan': 'SFR Acumulado',
            'FCR Plan': 'FCR Económico Acumulado',
            'Peso Final': 'Peso promedio',
        }

        def _darken_vivid(hex_color):
            """Take a hex color and return a darker, more saturated version."""
            import colorsys, re
            h_str = hex_color.lstrip('#')
            # Handle rgb() format too
            rgb_match = re.match(r'rgb\((\d+),\s*(\d+),\s*(\d+)\)', hex_color)
            if rgb_match:
                r, g, b = int(rgb_match.group(1)), int(rgb_match.group(2)), int(rgb_match.group(3))
            elif len(h_str) == 6:
                r, g, b = int(h_str[0:2], 16), int(h_str[2:4], 16), int(h_str[4:6], 16)
            else:
                return hex_color
            h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
            # More saturated + darker
            s = min(1.0, s * 1.4)
            l = max(0.1, l * 0.6)
            r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
            return f'#{int(r2*255):02x}{int(g2*255):02x}{int(b2*255):02x}'

        # Build lookup: production variable name fragment → trace color
        prod_trace_colors = {}
        for trace in fig.data:
            tname = getattr(trace, 'name', '') or ''
            tcolor = None
            if hasattr(trace, 'line') and trace.line and trace.line.color:
                tcolor = trace.line.color
            elif hasattr(trace, 'marker') and trace.marker and trace.marker.color:
                tcolor = trace.marker.color
            if tcolor and isinstance(tcolor, str):
                # Map variable name → color (trace names like "Lote 65SJ - SGR Acumulado")
                for paired_var in PROJ_PAIR_MAP.values():
                    if paired_var.lower() in tname.lower():
                        if paired_var not in prod_trace_colors:
                            prod_trace_colors[paired_var] = tcolor

        if proj_fecha_col and proj_batch_col:
            proj_batches = proj_df[proj_batch_col].unique()
            proj_var_cols = [c for c in proj_df.columns if c != proj_batch_col and c != proj_fecha_col]

            var_to_row = {}
            if std_var_subplots:
                for idx, v in enumerate(variables):
                    var_to_row[v] = idx + 1

            for bi, batch_id in enumerate(proj_batches):
                batch_data = proj_df[proj_df[proj_batch_col] == batch_id].sort_values(proj_fecha_col)
                if batch_data.empty:
                    continue

                for vi, var_col in enumerate(proj_var_cols):
                    trace_name = f"Batch {batch_id} - {var_col} (Plan)"

                    paired_prod_var = PROJ_PAIR_MAP.get(var_col)
                    target_row = var_to_row.get(paired_prod_var) if paired_prod_var else None

                    # Derive color from paired production trace
                    base_color = prod_trace_colors.get(paired_prod_var, '#78909C') if paired_prod_var else '#78909C'
                    color = _darken_vivid(base_color)

                    trace_kwargs = {}
                    if std_var_subplots and target_row:
                        trace_kwargs['row'] = target_row
                        trace_kwargs['col'] = 1

                    scatter = go.Scatter(
                        x=batch_data[proj_fecha_col],
                        y=batch_data[var_col],
                        mode='lines',
                        name=trace_name,
                        line=dict(color=color, width=2, dash='dash'),
                        opacity=0.85,
                        hovertemplate=(
                            f"<b>{trace_name}</b><br>"
                            f"Fecha: %{{x|%d-%m-%Y}}<br>"
                            f"Valor: %{{y:,.4f}}"
                            f"<extra></extra>"
                        ),
                        showlegend=True,
                    )

                    if trace_kwargs:
                        fig.add_trace(scatter, **trace_kwargs)
                    else:
                        fig.add_trace(scatter)

    # === Render Measurement Highlights ===
    if highlight_points:
        for p in highlight_points:
            # We add a dummy scatter trace just to show the marker
            fig.add_trace(go.Scatter(
                x=[p.get('x')],
                y=[p.get('y')],
                mode='markers',
                marker=dict(
                    symbol='cross',
                    size=16,
                    color='#FF4B4B',
                    line=dict(color='white', width=2)
                ),
                name='Punto Medido',
                showlegend=False,
                hoverinfo='skip'
            ))

    return fig
