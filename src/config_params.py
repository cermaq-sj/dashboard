import streamlit as st
import pandas as pd
import unicodedata

# Default group classification keywords
DEFAULT_GROUPS = {
    "Productivos": ['peso', 'weight', 'fcr', 'sfr', 'crecim', 'growth', 'sgr', 'gf3'],
    "Stock / Biomasa": ['cant', 'count', 'biomasa', 'biomass', 'numero', 'number', 'eliminado', 'densidad', 'perdida', 'transferido'],
    "Mortalidad": ['muer', 'mortal', 'deads', 'mort', 'aborto', 'deforme', 'desadaptado', 'descompuesto', 'embrionaria', 'exofialosis', 'maduro', 'micosis', 'muestras', 'nefrocalcinosis', 'operculo', 'rezagado', 'sin causa aparente'],
    "Alimentación": ['alim', 'feed', 'comida', 'ewos', 'skretting'],
    "Ambiental": ['temp', 'oxigen', 'oxygen', 'salin', 'turbidez', 'plomo', 'nitrito', 'nitrato', 'dureza', 'co2', 'amonio', 'alcalinidad', 'uta'],
    "Económico": ['venta', 'sales', 'costo', 'cost'],
}

def _normalize(s):
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower()

def _auto_group(col_name):
    """Auto-detect the group for a column based on keywords."""
    col_norm = _normalize(col_name)
    for group, keywords in DEFAULT_GROUPS.items():
        if any(_normalize(kw) in col_norm for kw in keywords):
            return group
    return "Otras Variables"


def get_numeric_columns_info(db_manager, table_name: str) -> list:
    """
    Returns a list of dicts with info about each numeric column in the table.
    Each dict: {'column': str, 'data_min': float, 'data_max': float}
    """
    try:
        desc = db_manager.con.execute(f"DESCRIBE {table_name}").fetchall()
    except Exception:
        return []

    numeric_types = ['DOUBLE', 'FLOAT', 'DECIMAL', 'BIGINT', 'INTEGER', 'INT',
                     'HUGEINT', 'SMALLINT', 'TINYINT']
    # Columns to exclude (identifiers, not metrics)
    exclude_keywords = ['source', 'lote', 'batch', 'unidad', 'unit', 'jaula',
                        'cage', 'depto', 'dep', 'area', 'sect', 'sheet',
                        'horario', 'lugar', 'semana', 'week']

    results = []
    for col_info in desc:
        col_name = col_info[0]
        col_type = col_info[1].upper()

        if not any(t in col_type for t in numeric_types):
            continue

        col_lower = col_name.lower()
        if any(kw in col_lower for kw in exclude_keywords):
            continue

        # Get min/max from data
        try:
            row = db_manager.con.execute(
                f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM {table_name}'
            ).fetchone()
            data_min = row[0] if row[0] is not None else 0.0
            data_max = row[1] if row[1] is not None else 0.0
        except Exception:
            data_min, data_max = 0.0, 0.0

        results.append({
            'column': col_name,
            'data_min': float(data_min),
            'data_max': float(data_max),
        })

    return results


def _init_param_config(db_manager):
    """Initialize param_config in session_state if not present."""
    if 'param_config' not in st.session_state:
        st.session_state.param_config = {}

    for tbl in ['fishtalk_data', 'mediciones_data']:
        if tbl not in st.session_state.param_config:
            cols_info = get_numeric_columns_info(db_manager, tbl)
            tbl_config = {}
            for info in cols_info:
                col = info['column']
                d_min = info['data_min']
                d_max = info['data_max']

                # Special default: FCR Económico -> min = 0
                user_min = d_min
                if 'fcr' in col.lower() and 'econ' in col.lower():
                    user_min = 0.0

                tbl_config[col] = {
                    'alias': col,        # display name (editable)
                    'min': user_min,     # user-set minimum
                    'max': d_max,        # user-set maximum
                    'data_min': d_min,   # original data min
                    'data_max': d_max,   # original data max
                    'visible': True,     # show/hide in sidebar
                    'grupo': _auto_group(col) if tbl == 'fishtalk_data' else '',
                }
            st.session_state.param_config[tbl] = tbl_config

    # Initialize KPI thresholds config
    if 'kpi_config' not in st.session_state.param_config:
        try:
            kpi_thresholds = db_manager.get_kpi_thresholds()
            kpi_config = {}
            for tipo_kpi, dept_vals in kpi_thresholds.items():
                for dept, val in dept_vals.items():
                    key = f"{tipo_kpi}|{dept}"
                    kpi_config[key] = {
                        'tipo_kpi': tipo_kpi,
                        'departamento': dept,
                        'umbral': val,
                        'umbral_original': val,
                        'visible': True,
                    }
            st.session_state.param_config['kpi_config'] = kpi_config
        except Exception:
            st.session_state.param_config['kpi_config'] = {}


def render_config_tab(db_manager):
    """Render the full Configuration tab UI."""
    _init_param_config(db_manager)

    st.header("⚙️ Configuración de Parámetros")
    st.caption("Ajusta rangos y nombres de las variables. Los cambios se aplican automáticamente a filtros, gráficos y KPIs.")

    # --- Production Variables ---
    _render_table_config("Producción (Excel Pesado)", "fishtalk_data")

    st.markdown("---")

    # --- Mediciones Variables ---
    _render_table_config("Mediciones (Ambiental)", "mediciones_data")

    st.markdown("---")

    # --- KPIs ---
    _render_kpi_config(db_manager)


def _render_table_config(title: str, table_key: str):
    """Render the config editor for one table."""
    config = st.session_state.param_config.get(table_key, {})
    if not config:
        st.info(f"No hay variables numéricas en {title}.")
        return

    st.subheader(title)

    rows = []
    for col_name, cfg in config.items():
        rows.append({
            'Visible': cfg.get('visible', True),
            'Variable Original': col_name,
            'Nombre Personalizado': cfg['alias'],
            'Grupo': cfg.get('grupo', ''),
            'Desde': cfg['min'],
            'Hasta': cfg['max'],
            'Mín. Datos': cfg['data_min'],
            'Máx. Datos': cfg['data_max'],
        })

    df = pd.DataFrame(rows)

    edited_df = st.data_editor(
        df,
        column_config={
            'Visible': st.column_config.CheckboxColumn(
                '👁️',
                width='small',
                default=True,
            ),
            'Variable Original': st.column_config.TextColumn(
                'Variable Original',
                disabled=True,
                width='medium',
            ),
            'Nombre Personalizado': st.column_config.TextColumn(
                'Nombre Personalizado',
                width='medium',
            ),
            'Grupo': st.column_config.TextColumn(
                'Grupo',
                width='small',
                help='Carpeta donde aparecerá en el sidebar',
            ),
            'Desde': st.column_config.NumberColumn(
                'Desde',
                format="%.4f",
                width='small',
            ),
            'Hasta': st.column_config.NumberColumn(
                'Hasta',
                format="%.4f",
                width='small',
            ),
            'Mín. Datos': st.column_config.NumberColumn(
                'Mín. Datos',
                disabled=True,
                format="%.4f",
                width='small',
            ),
            'Máx. Datos': st.column_config.NumberColumn(
                'Máx. Datos',
                disabled=True,
                format="%.4f",
                width='small',
            ),
        },
        use_container_width=True,
        hide_index=True,
        num_rows='fixed',
        key=f"config_editor_{table_key}",
    )

    if edited_df is not None:
        for _, row in edited_df.iterrows():
            col_name = row['Variable Original']
            if col_name in st.session_state.param_config[table_key]:
                entry = st.session_state.param_config[table_key][col_name]
                entry['alias'] = row['Nombre Personalizado']
                entry['min'] = float(row['Desde'])
                entry['max'] = float(row['Hasta'])
                entry['visible'] = bool(row['Visible'])
                entry['grupo'] = str(row['Grupo']).strip() if pd.notna(row['Grupo']) else ''


def get_alias_map(table_key: str = 'fishtalk_data') -> dict:
    """Returns {original_col: alias} for display renaming."""
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {col: cfg['alias'] for col, cfg in config.items() if cfg['alias'] != col}


def get_range_filters(table_key: str = 'fishtalk_data') -> dict:
    """
    Returns {col_name: (min, max)} for columns where the user has
    modified the range from the data defaults.
    """
    config = st.session_state.get('param_config', {}).get(table_key, {})
    modified = {}
    for col, cfg in config.items():
        # Only include if user changed from data defaults
        if cfg['min'] != cfg['data_min'] or cfg['max'] != cfg['data_max']:
            modified[col] = (cfg['min'], cfg['max'])
    return modified


def _render_kpi_config(db_manager):
    """Render the KPI thresholds configuration editor."""
    kpi_config = st.session_state.param_config.get('kpi_config', {})
    if not kpi_config:
        st.info("No hay KPIs cargados. Sube el archivo 'KPIs y Proyecciones por Batch'.")
        return

    st.subheader("📊 KPIs (Umbrales)")
    st.caption("Edita los valores de umbral para cada KPI y departamento.")

    rows = []
    for key, cfg in kpi_config.items():
        rows.append({
            'Visible': cfg.get('visible', True),
            'Tipo KPI': cfg['tipo_kpi'],
            'Departamento': cfg['departamento'],
            'Umbral (Menor a)': cfg['umbral'],
            'Valor Original': cfg['umbral_original'],
        })

    df = pd.DataFrame(rows)

    edited_df = st.data_editor(
        df,
        column_config={
            'Visible': st.column_config.CheckboxColumn(
                '👁️',
                width='small',
                default=True,
            ),
            'Tipo KPI': st.column_config.TextColumn(
                'Tipo KPI',
                disabled=True,
                width='medium',
            ),
            'Departamento': st.column_config.TextColumn(
                'Departamento',
                disabled=True,
                width='small',
            ),
            'Umbral (Menor a)': st.column_config.NumberColumn(
                'Umbral (Menor a)',
                format="%.6f",
                width='small',
            ),
            'Valor Original': st.column_config.NumberColumn(
                'Valor Original',
                disabled=True,
                format="%.6f",
                width='small',
            ),
        },
        use_container_width=True,
        hide_index=True,
        num_rows='fixed',
        key="config_editor_kpis",
    )

    if edited_df is not None:
        updated = False
        for _, row in edited_df.iterrows():
            key = f"{row['Tipo KPI']}|{row['Departamento']}"
            if key in st.session_state.param_config['kpi_config']:
                entry = st.session_state.param_config['kpi_config'][key]
                new_val = float(row['Umbral (Menor a)'])
                if entry['umbral'] != new_val:
                    entry['umbral'] = new_val
                    updated = True
                entry['visible'] = bool(row['Visible'])

        # Update the DuckDB table if values changed
        if updated:
            try:
                for key, cfg in st.session_state.param_config['kpi_config'].items():
                    db_manager.con.execute(
                        "UPDATE kpi_thresholds SET menor_a = ? WHERE tipo_kpi = ? AND departamento = ?",
                        [cfg['umbral'], cfg['tipo_kpi'], cfg['departamento']]
                    )
            except Exception:
                pass


def get_kpi_config_thresholds() -> dict:
    """
    Returns KPI thresholds from the config (user-editable values).
    Only includes visible KPIs.
    Format: {tipo_kpi: {departamento: umbral_value}}
    """
    kpi_config = st.session_state.get('param_config', {}).get('kpi_config', {})
    result = {}
    for key, cfg in kpi_config.items():
        if not cfg.get('visible', True):
            continue
        tipo = cfg['tipo_kpi']
        dept = cfg['departamento']
        val = cfg['umbral']
        if tipo not in result:
            result[tipo] = {}
        result[tipo][dept] = val
    return result


def get_hidden_variables(table_key: str = 'fishtalk_data') -> set:
    """Returns a set of column names that the user has hidden in config."""
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {col for col, cfg in config.items() if not cfg.get('visible', True)}


def get_variable_group_overrides(table_key: str = 'fishtalk_data') -> dict:
    """
    Returns {column_name: grupo} for columns where the user set a custom group.
    Only returns entries where grupo is non-empty.
    """
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {col: cfg['grupo'] for col, cfg in config.items()
            if cfg.get('grupo', '').strip()}
