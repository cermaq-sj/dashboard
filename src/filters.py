
import streamlit as st
import pandas as pd
from datetime import datetime, time
from src.config_params import get_alias_map, get_hidden_variables, get_variable_group_overrides

def render_filters(db_manager, mediciones_meta=None, mediciones_date_bounds=None, kpi_thresholds=None, proyecciones_meta=None):
    """
    Renders filter sidebar and returns a dictionary of selected filters.
    
    Args:
        db_manager: DBManager instance (used for querying unique values)
        mediciones_meta: Dict of sheets/places/columns for Mediciones file
        mediciones_date_bounds: Tuple (min, max) for Mediciones date range
    """
    st.sidebar.header("Filtros")
    
    filters = {
        'batches': [],
        'depts': [],
        'units': [],
        'date_range': [],
        'days_range': [],
        'variables': [],
        'sum_units': False,
        'mediciones_places': [],
        'mediciones_vars': [],
        'mediciones_date_range': [],
        'active_kpis': [],
        'proyecciones_vars': [],
    }
    
    # helper to get column names if not hardcoded (we rely on db_manager's heuristics mostly)
    # but we need to know what columns are available to populate variables
    # Let's get a list of all columns first
    try:
        all_cols_df = db_manager.query("DESCRIBE fishtalk_data")
        all_cols = all_cols_df['column_name'].tolist() if not all_cols_df.empty else []
    except:
        all_cols = []
    
    # --- 1. Batch Filter (Lote) ---
    # Primary filter.
    batches = db_manager.get_unique_values("Lote")
    selected_batches = st.sidebar.multiselect("Lotes (Batch)", options=batches, default=batches[:1] if batches else None)
    filters['batches'] = selected_batches
    
    # --- 2. Date Range & Days Range ---
    # Logic: If batches are selected, we could limit the range to those batches.
    # For now, let's just get global min/max or filtered if possible. 
    # To keep it fast, we might just use global limits for the slider bounds.
    
    min_date, max_date = db_manager.get_min_max("Fecha")
    
    if min_date and max_date:
        # Check types
        if isinstance(min_date, str):
            min_date = pd.to_datetime(min_date)
        if isinstance(max_date, str):
            max_date = pd.to_datetime(max_date)
            
        # Slider
        date_range = st.sidebar.date_input(
            "Rango de Fechas",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        filters['date_range'] = date_range


    # Days since input
    min_days, max_days = db_manager.get_min_max("Days") # Assuming column name like 'Days since first input'
    if min_days is not None and max_days is not None:
         filters['days_range'] = st.sidebar.slider(
             "Días de Cultivo (Days)", 
             int(min_days), 
             int(max_days), 
             (int(min_days), int(max_days))
         )

    # Granularity toggle: Day vs Week
    granularity = st.sidebar.radio(
        "📅 Agrupación temporal",
        ["Día", "Semana"],
        horizontal=True,
        key="time_granularity"
    )
    filters['granularity'] = granularity

    st.sidebar.markdown("---")

    # --- 3. Department & Unit (Cascading) ---
    depts = db_manager.get_unique_values("Departamento")
    
    all_depts = st.sidebar.checkbox("Todos los departamentos", value=False, key="all_depts")
    if all_depts:
        selected_depts = depts
        st.sidebar.multiselect("Departamentos", options=depts, default=depts, disabled=True, key="depts_display")
    else:
        selected_depts = st.sidebar.multiselect("Departamentos", options=depts)
    filters['depts'] = selected_depts
    
    # Cascade: Get units. If depts selected, filter units by those depts.
    if selected_depts:
        depts_str = "', '".join([str(d) for d in selected_depts])
        dept_col = next((c for c in all_cols if 'depto' in c.lower() or 'dep' in c.lower() or 'area' in c.lower()), 'Departamento')
        unit_col = next((c for c in all_cols if 'unidad' in c.lower() or 'unit' in c.lower() or 'jaula' in c.lower()), 'Unidad')
        
        units_query = f"SELECT DISTINCT \"{unit_col}\" FROM fishtalk_data WHERE \"{dept_col}\" IN ('{depts_str}') ORDER BY \"{unit_col}\""
        units = db_manager.query(units_query)
        if not units.empty:
             available_units = units.iloc[:, 0].tolist()
        else:
             available_units = []
    else:
        available_units = db_manager.get_unique_values("Unidad")
    
    all_units = st.sidebar.checkbox("Todas las unidades", value=False, key="all_units")
    if all_units:
        selected_units = available_units
        st.sidebar.multiselect("Unidades", options=available_units, default=available_units, disabled=True, key="units_display")
    else:
        selected_units = st.sidebar.multiselect("Unidades", options=available_units, default=available_units if selected_depts else None)
    filters['units'] = selected_units
    
    # Sum/Avg Units Toggles removed per user request
    filters['sum_units'] = False
    filters['avg_units'] = False
    
    st.sidebar.markdown("---")

    # --- 4. Variables (Grouped) ---
    # Definition of groups and keywords to find them
    # Removed "Mediciones" from here as it now has its own section
    variable_groups = {
        "Productivos": ['peso', 'weight', 'fcr', 'sfr', 'crecim', 'growth', 'sgr', 'gf3'],
        "Stock / Biomasa": ['cant', 'count', 'biomasa', 'biomass', 'numero', 'number', 'eliminado', 'densidad', 'perdida', 'transferido'],
        "Mortalidad": ['muer', 'mortal', 'deads', 'mort', 'aborto', 'deforme', 'desadaptado', 'descompuesto', 'embrionaria', 'exofialosis', 'maduro', 'micosis', 'muestras', 'nefrocalcinosis', 'operculo', 'rezagado', 'sin causa aparente'],
        "Alimentación": ['alim', 'feed', 'comida', 'ewos', 'skretting'],
        "Ambiental": ['temp', 'oxigen', 'oxygen', 'salin', 'turbidez', 'plomo', 'nitrito', 'nitrato', 'dureza', 'co2', 'amonio', 'alcalinidad', 'uta'],
        "Económico": ['venta', 'sales', 'costo', 'cost']
    }
    
    # Classify available columns
    grouped_cols = {k: [] for k in variable_groups}
    other_cols = []
    
    # Exclude structural columns
    exclude = ['fecha', 'date', 'lote', 'batch', 'unidad', 'unit', 'jaula', 
               'depto', 'departamento', 'area', 'source', 
               'nombre de grupo', 'generaci', 'especies', 'cliente', 'qtl', 'seleccion genomica']
    
    # Get column types efficiently
    numeric_types = ['DOUBLE', 'FLOAT', 'DECIMAL', 'BIGINT', 'INTEGER', 'INT', 'HUGEINT', 'SMALLINT', 'TINYINT', 'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT']
    try:
        desc = db_manager.query("DESCRIBE fishtalk_data")
        col_type_map = {row['column_name']: row['column_type'] for _, row in desc.iterrows()}
    except Exception as e:
        col_type_map = {}
        print(f"Error getting schema: {e}")

    # Helper to normalize
    import unicodedata
    def normalize(s):
        nfkd = unicodedata.normalize('NFKD', s)
        return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower()

    # Special handling for Mediciones Columns
    # Use provided metadata (already computed/cached in app) to avoid extra queries.
    mediciones_meta = mediciones_meta or {}
    mediciones_keywords = ['aluminio', 'cobre', 'hierro', 'plomo', 'horario', 'lugar de muestreo']
    mediciones_cols = []

    for col in all_cols:
        col_norm = normalize(col)
        
        # 1. Check exclusions
        is_excluded = False
        for ex in exclude:
            if normalize(ex) in col_norm:
                is_excluded = True
                break
        if is_excluded: continue
            
        # 2. Check if numeric
        ctype = col_type_map.get(col, '').upper()
        is_numeric = any(t in ctype for t in numeric_types)
        if not is_numeric: continue

        # 3. Classify
        # Check if it belongs to Mediciones (by keyword for now, or if we had a mapping)
        # Since we don't know which column belongs to which sheet without querying specifically,
        # we will use keywords for the general grouping if metadata is missing,
        # BUT the new requirement is to use metadata for sheets.
        
        # We will separate "Mediciones" columns from the general bucket
        is_mediciones = any(normalize(k) in col_norm for k in mediciones_keywords)
        
        if is_mediciones:
            mediciones_cols.append(col)
            continue # Don't add to other groups

        found = False
        for group, keywords in variable_groups.items():
            if any(normalize(key) in col_norm for key in keywords):
                grouped_cols[group].append(col)
                found = True
                break
        
        if not found:
            other_cols.append(col)

    # Apply config: hide variables and reassign groups
    hidden_vars = get_hidden_variables('fishtalk_data')
    group_overrides = get_variable_group_overrides('fishtalk_data')
    
    # Remove hidden variables from all groups
    for group in list(grouped_cols.keys()):
        grouped_cols[group] = [c for c in grouped_cols[group] if c not in hidden_vars]
    other_cols = [c for c in other_cols if c not in hidden_vars]
    
    # Apply group overrides: move variables to user-specified groups
    for col, new_group in group_overrides.items():
        if col in hidden_vars:
            continue
        # Remove from current group
        for group in list(grouped_cols.keys()):
            if col in grouped_cols[group]:
                grouped_cols[group].remove(col)
        if col in other_cols:
            other_cols.remove(col)
        # Add to new group (create if needed)
        if new_group not in grouped_cols:
            grouped_cols[new_group] = []
        if col not in grouped_cols[new_group]:
            grouped_cols[new_group].append(col)

    # Inject our dynamic column so the user can select it
    # We put it in the Economic group
    grouped_cols["Económico"].append("FCR Económico Acumulado")
    grouped_cols["Productivos"].append("FCR Biológico Acumulado")
    grouped_cols["Productivos"].append("GF3 Acumulado")
    grouped_cols["Productivos"].append("SGR Acumulado")
    grouped_cols["Productivos"].append("SFR Acumulado")
    grouped_cols["Productivos"].append("% Mortalidad Acumulada")
    grouped_cols["Productivos"].append("% Mortalidad diaria")
    grouped_cols["Productivos"].append("% Pérdida Acumulada")
    grouped_cols["Productivos"].append("Pérdida diaria %")
    grouped_cols["Productivos"].append("% Eliminación Acumulada")
    grouped_cols["Productivos"].append("Eliminación diaria %")
    grouped_cols["Productivos"].append("Peso promedio")

    # Mortality by Cause group
    cause_names = [
        'Embrionaria', 'Deforme Embrionaria', 'Micosis', 'Daño Mecánico Otros',
        'Desadaptado', 'Deforme', 'Descompuesto', 'Aborto', 'Daño Mecánico',
        'Sin causa Aparente', 'Maduro', 'Muestras', 'Operculo Corto',
        'Rezagado', 'Nefrocalcinosis', 'Exofialosis', 'Daño Mecánico por Muestreo',
    ]
    grouped_cols["Mortalidad por Causa"] = [f"% Mortalidad {c} Diaria" for c in cause_names] + [f"% Mortalidad {c} Acumulada" for c in cause_names]

    # Render Multiselects per group
    selected_vars = []
    
    st.sidebar.markdown("#### Variables")
    
    # --- Denominator Logic for UI Stars ---
    # Map variables to their base denominator type
    DENOMINATOR_MAP = {
        "% Mortalidad Acumulada": "Poblacion Inicial",
        "% Pérdida Acumulada": "Poblacion Inicial",
        "% Eliminación Acumulada": "Poblacion Inicial",
        "% Mortalidad diaria": "Poblacion Diaria",
        "Pérdida diaria %": "Poblacion Diaria",
        "Eliminación diaria %": "Poblacion Diaria",
    }
    for c in cause_names:
        DENOMINATOR_MAP[f"% Mortalidad {c} Acumulada"] = "Poblacion Inicial"
        DENOMINATOR_MAP[f"% Mortalidad {c} Diaria"] = "Poblacion Diaria"
        
    # Find active denominators from session state (since Streamlit runs top-to-bottom, 
    # the current widget states are in st.session_state from the previous run)
    active_denominators = set()
    for group in grouped_cols.keys():
        key = f"group_{group}"
        if key in st.session_state:
            for v in st.session_state[key]:
                if v in DENOMINATOR_MAP:
                    active_denominators.add(DENOMINATOR_MAP[v])
    
    # Get alias map for display
    alias_map = get_alias_map('fishtalk_data')
    
    def _fmt(col):
        base_name = alias_map.get(col, col)
        # If this unselected column shares a denominator with a selected column, add a star
        if col in DENOMINATOR_MAP and DENOMINATOR_MAP[col] in active_denominators:
            # Only add star if it's not actually selected (optional, but requested for related vars)
            # Actually, adding a star to *all* vars with that denominator makes them pop together.
            return f"⭐ {base_name}"
        return base_name
    
    # --- Trio auto-clear logic ---
    # Determine what is currently selected inside session_state (before widgets render)
    trio_vars = {"% pérdida acumulada", "% eliminación acumulada", "% mortalidad acumulada"}
    current_selected_lower = set()
    for group in grouped_cols.keys():
        key = f"group_{group}"
        for v in st.session_state.get(key, []):
            current_selected_lower.add(v.strip().lower())
            
    for v in st.session_state.get("group_Otras Variables", []):
        current_selected_lower.add(v.strip().lower())
        
    current_has_trio = trio_vars.issubset(current_selected_lower)
    
    if st.session_state.get('trio_was_selected', False) and not current_has_trio:
        # Transition: Trio was selected, now it's not. Clear all variables.
        for group in grouped_cols.keys():
            key = f"group_{group}"
            if key in st.session_state:
                st.session_state[key] = []
        if "group_Otras Variables" in st.session_state:
            st.session_state["group_Otras Variables"] = []
        
        current_has_trio = False # Update since we cleared everything
        
    st.session_state.trio_was_selected = current_has_trio

    # 1. Standard Groups
    for group, cols in grouped_cols.items():
        if cols:
            with st.sidebar.expander(group, expanded=False):
                sel = st.multiselect("Seleccionar", options=sorted(cols), format_func=_fmt, key=f"group_{group}", label_visibility="collapsed")
                selected_vars.extend(sel)
            
    if other_cols:
        with st.sidebar.expander("Otras Variables"):
            sel = st.multiselect("Seleccionar", options=sorted(other_cols))
            selected_vars.extend(sel)

    # Add FCR/GF3/SGR View Toggle directly to the sidebar if an Acumulado is selected
    has_cause_metric = any(v.startswith("% Mortalidad") and (v.endswith("Diaria") or v.endswith("Acumulada")) and v not in ("% Mortalidad diaria", "% Mortalidad Acumulada") for v in selected_vars)
    if (has_cause_metric or
        "FCR Económico Acumulado" in selected_vars or 
        "FCR Biológico Acumulado" in selected_vars or 
        "GF3 Acumulado" in selected_vars or 
        "SGR Acumulado" in selected_vars or
        "SFR Acumulado" in selected_vars or
        "% Mortalidad Acumulada" in selected_vars or
        "% Mortalidad diaria" in selected_vars or
        "% Pérdida Acumulada" in selected_vars or
        "Pérdida diaria %" in selected_vars or
        "% Eliminación Acumulada" in selected_vars or
        "Eliminación diaria %" in selected_vars or
        "Peso promedio" in selected_vars):
        if 'fcr_view_mode' not in st.session_state:
            st.session_state.fcr_view_mode = "Vista general"
            
        st.sidebar.markdown("---")
        # primary type will trigger the dark green CSS injected in app.py
        btn_type = "primary" if st.session_state.fcr_view_mode == "Vista general" else "secondary"
        if st.sidebar.button(st.session_state.fcr_view_mode, type=btn_type, help="Alternar vista del FCR", key="fcr_sidebar_btn"):
            if st.session_state.fcr_view_mode == "Vista general":
                st.session_state.fcr_view_mode = "Vista individual"
            else:
                st.session_state.fcr_view_mode = "Vista general"
            st.rerun()

    # 2. Mediciones Sheets Sections (Moved to Bottom as Requested)
    if mediciones_meta:
        filters['mediciones_places'] = []
        filters['mediciones_vars'] = [] # Separate list for the new chart
        

        
        # Average toggle
        filters['mediciones_avg'] = st.sidebar.checkbox(
            "📊 Promediar por día",
            value=False,
            key="med_avg",
            help="Promedia los valores del mismo día y lugar de muestreo"
        )
        # Sort sheets to prevent UI jumping
        sorted_sheets = sorted(mediciones_meta.keys())
        
        for sheet in sorted_sheets:
            # Metadata structure: {'places': [], 'columns': []}
            sheet_data = mediciones_meta[sheet]
            places = sheet_data.get('places', [])
            cols = sheet_data.get('columns', [])
            
            # Ensure consistent key for expander
            with st.sidebar.expander(f"📁 {sheet}", expanded=False):
                if sheet == "Metales" and places:
                    # Selector de Lugar (Multiselect now)
                    places_sel = st.multiselect(
                        f"Lugar de muestreo ({sheet})", 
                        options=places, 
                        key=f"place_{sheet}",
                        placeholder="Seleccionar lugares..."
                    )
                    
                    # Store selected places
                    if places_sel:
                        filters['mediciones_places'].extend(places_sel)
                else:
                    # Automatically select 'General' or other available places for non-Metales sheets
                    if places:
                        filters['mediciones_places'].extend(places)
                
                # Variables (Always visible now)
                # Use dynamic columns from metadata
                st.caption("Variables:")
                if cols:
                    if sheet == "Smolt":
                        # Split columns into S1 and S2
                        s1_cols = [c for c in cols if 'S1' in c or ' S1' in c]
                        s2_cols = [c for c in cols if 'S2' in c or ' S2' in c]
                        other_cols = [c for c in cols if c not in s1_cols and c not in s2_cols]

                        if s1_cols:
                            sel_s1 = st.multiselect("Variables S1", options=sorted(s1_cols), key=f"vars_{sheet}_s1")
                            filters['mediciones_vars'].extend(sel_s1)
                        if s2_cols:
                            sel_s2 = st.multiselect("Variables S2", options=sorted(s2_cols), key=f"vars_{sheet}_s2")
                            filters['mediciones_vars'].extend(sel_s2)
                        if other_cols:
                            sel_other = st.multiselect("Otras Variables", options=sorted(other_cols), key=f"vars_{sheet}_other")
                            filters['mediciones_vars'].extend(sel_other)
                    else:
                        sel_med = st.multiselect(
                            "Seleccionar Variables", 
                            options=sorted(cols),
                            key=f"vars_{sheet}",
                            label_visibility="collapsed"
                        )
                        filters['mediciones_vars'].extend(sel_med)
                else:
                    st.caption("No variables detected.")

        # Add Date Picker for Mediciones (Moved to Bottom)
        if mediciones_date_bounds and mediciones_date_bounds[0]:
            min_date, max_date = mediciones_date_bounds
            if min_date and max_date:
                start_d = pd.to_datetime(min_date).date()
                end_d = pd.to_datetime(max_date).date()
                
                st.caption("Rango de fechas (Mediciones)")
                filters['mediciones_date_range'] = st.date_input(
                    "Seleccionar rango",
                    value=(start_d, end_d),
                    min_value=start_d,
                    max_value=end_d,
                    key="med_date_range"
                )
            
    # --- 5. KPIs Section ---
    if kpi_thresholds:
        st.sidebar.markdown("---")
        with st.sidebar.expander("📊 KPIs (Umbrales)", expanded=False):
            st.caption("Selecciona los KPIs para mostrar líneas de umbral en el gráfico.")
            available_kpis = sorted(kpi_thresholds.keys())
            sel_kpis = st.multiselect(
                "Tipo KPI",
                options=available_kpis,
                key="kpi_select",
                label_visibility="collapsed"
            )
            filters['active_kpis'] = sel_kpis

    # --- 6. Proyecciones Section ---
    if proyecciones_meta and proyecciones_meta.get('variables'):
        with st.sidebar.expander("📈 Proyecciones", expanded=False):
            st.caption("Superponer curvas de proyección (Plan) sobre el gráfico principal.")
            proj_vars = sorted(proyecciones_meta['variables'])
            sel_proj = st.multiselect(
                "Variables de Proyección",
                options=proj_vars,
                key="proj_vars_select",
                label_visibility="collapsed"
            )
            filters['proyecciones_vars'] = sel_proj

    filters['variables'] = selected_vars
    
    return filters
