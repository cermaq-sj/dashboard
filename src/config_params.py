import copy
import unicodedata

import pandas as pd
import streamlit as st


CONFIG_STORE_KEY = "param_config_v1"

# Default group classification keywords
DEFAULT_GROUPS = {
    "Productivos": ['peso', 'weight', 'fcr', 'sfr', 'crecim', 'growth', 'sgr', 'gf3'],
    "Stock / Biomasa": ['cant', 'count', 'biomasa', 'biomass', 'numero', 'number', 'eliminado', 'densidad', 'perdida', 'transferido'],
    "Mortalidad": ['muer', 'mortal', 'deads', 'mort', 'aborto', 'deforme', 'desadaptado', 'descompuesto', 'embrionaria', 'exofialosis', 'maduro', 'micosis', 'muestras', 'nefrocalcinosis', 'operculo', 'rezagado', 'sin causa aparente'],
    "Alimentación": ['alim', 'feed', 'comida', 'ewos', 'skretting'],
    "Ambiental": ['temp', 'oxigen', 'oxygen', 'salin', 'turbidez', 'plomo', 'nitrito', 'nitrato', 'dureza', 'co2', 'amonio', 'alcalinidad', 'uta'],
    "Económico": ['venta', 'sales', 'costo', 'cost'],
}
DEFAULT_EXTRA_GROUPS = ["Mortalidad por Causa", "Otras Variables"]


def _normalize(text):
    text = unicodedata.normalize('NFKD', str(text))
    return ''.join(c for c in text if not unicodedata.combining(c)).lower().strip()


def _sanitize_folder_name(name) -> str:
    clean = str(name or '').strip()
    return clean[:60]


def _auto_group(col_name):
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
                     'HUGEINT', 'SMALLINT', 'TINYINT', 'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT']

    exclude_keywords = ['source', 'lote', 'batch', 'unidad', 'unit', 'jaula',
                        'cage', 'depto', 'dep', 'area', 'sect', 'sheet',
                        'horario', 'lugar', 'semana', 'week']

    results = []
    for col_info in desc:
        col_name = col_info[0]
        col_type = col_info[1].upper()

        if not any(t in col_type for t in numeric_types):
            continue

        if any(kw in col_name.lower() for kw in exclude_keywords):
            continue

        try:
            row = db_manager.con.execute(
                f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM {table_name}'
            ).fetchone()
            data_min = row[0] if row and row[0] is not None else 0.0
            data_max = row[1] if row and row[1] is not None else 0.0
        except Exception:
            data_min, data_max = 0.0, 0.0

        results.append({
            'column': col_name,
            'data_min': float(data_min),
            'data_max': float(data_max),
        })

    return results


def _default_folder_state():
    ordered = list(DEFAULT_GROUPS.keys()) + DEFAULT_EXTRA_GROUPS
    return {
        'fishtalk_data': {
            name: {'order': idx + 1, 'visible': True}
            for idx, name in enumerate(ordered)
        }
    }


def _to_float(value, fallback):
    try:
        return float(value)
    except Exception:
        return float(fallback)


def _to_int(value, fallback):
    try:
        return int(value)
    except Exception:
        return int(fallback)


def _build_default_param_config(db_manager):
    config = {}

    for tbl in ['fishtalk_data', 'mediciones_data']:
        cols_info = get_numeric_columns_info(db_manager, tbl)
        tbl_config = {}
        for idx, info in enumerate(cols_info):
            col = info['column']
            d_min = info['data_min']
            d_max = info['data_max']

            user_min = 0.0 if ('fcr' in col.lower() and 'econ' in col.lower()) else d_min
            grp = _auto_group(col) if tbl == 'fishtalk_data' else ''

            tbl_config[col] = {
                'alias': col,
                'min': float(user_min),
                'max': float(d_max),
                'data_min': float(d_min),
                'data_max': float(d_max),
                'visible': True,
                'grupo': grp,
                'orden': idx + 1,
            }
        config[tbl] = tbl_config

    try:
        kpi_thresholds = db_manager.get_kpi_thresholds()
    except Exception:
        kpi_thresholds = {}

    kpi_config = {}
    for tipo_kpi, dept_vals in kpi_thresholds.items():
        for dept, val in dept_vals.items():
            key = f"{tipo_kpi}|{dept}"
            kpi_config[key] = {
                'tipo_kpi': tipo_kpi,
                'departamento': dept,
                'umbral': float(val),
                'umbral_original': float(val),
                'visible': True,
            }
    config['kpi_config'] = kpi_config
    config['folder_config'] = _default_folder_state()
    return config


def _ensure_folder_consistency(config):
    folder_cfg = config.setdefault('folder_config', {})
    table_folder_cfg = folder_cfg.setdefault('fishtalk_data', {})

    for idx, name in enumerate(list(DEFAULT_GROUPS.keys()) + DEFAULT_EXTRA_GROUPS):
        if name not in table_folder_cfg:
            table_folder_cfg[name] = {'order': idx + 1, 'visible': True}

    fish_cfg = config.get('fishtalk_data', {})
    max_order = max([v.get('order', 0) for v in table_folder_cfg.values()] + [0])

    for col_name, entry in fish_cfg.items():
        grp = _sanitize_folder_name(entry.get('grupo', '')) or _auto_group(col_name)
        if grp not in table_folder_cfg:
            max_order += 1
            table_folder_cfg[grp] = {'order': max_order, 'visible': True}
        entry['grupo'] = grp
        entry['orden'] = _to_int(entry.get('orden', 999999), 999999)

    if 'Otras Variables' not in table_folder_cfg:
        max_order += 1
        table_folder_cfg['Otras Variables'] = {'order': max_order, 'visible': True}

    ordered = sorted(table_folder_cfg.items(), key=lambda kv: _to_int(kv[1].get('order', 999999), 999999))
    normalized = {}
    for idx, (name, state) in enumerate(ordered):
        normalized[name] = {
            'order': idx + 1,
            'visible': bool(state.get('visible', True)),
        }
    folder_cfg['fishtalk_data'] = normalized


def _merge_saved_config(default_cfg, saved_cfg):
    merged = copy.deepcopy(default_cfg)
    if not isinstance(saved_cfg, dict):
        _ensure_folder_consistency(merged)
        return merged

    for tbl in ['fishtalk_data', 'mediciones_data']:
        saved_tbl = saved_cfg.get(tbl, {})
        if not isinstance(saved_tbl, dict):
            continue
        for col, entry in merged.get(tbl, {}).items():
            saved_entry = saved_tbl.get(col)
            if not isinstance(saved_entry, dict):
                continue
            entry['alias'] = str(saved_entry.get('alias', entry['alias']))
            entry['min'] = _to_float(saved_entry.get('min', entry['min']), entry['min'])
            entry['max'] = _to_float(saved_entry.get('max', entry['max']), entry['max'])
            entry['visible'] = bool(saved_entry.get('visible', entry['visible']))
            entry['orden'] = _to_int(saved_entry.get('orden', entry['orden']), entry['orden'])
            if tbl == 'fishtalk_data':
                entry['grupo'] = _sanitize_folder_name(saved_entry.get('grupo', entry['grupo'])) or entry['grupo']

    saved_kpi = saved_cfg.get('kpi_config', {})
    if isinstance(saved_kpi, dict):
        for key, entry in merged.get('kpi_config', {}).items():
            saved_entry = saved_kpi.get(key)
            if not isinstance(saved_entry, dict):
                continue
            entry['umbral'] = _to_float(saved_entry.get('umbral', entry['umbral']), entry['umbral'])
            entry['visible'] = bool(saved_entry.get('visible', entry['visible']))

    saved_folder = (
        saved_cfg.get('folder_config', {})
        if isinstance(saved_cfg.get('folder_config', {}), dict)
        else {}
    )
    saved_fish_folder = (
        saved_folder.get('fishtalk_data', {})
        if isinstance(saved_folder.get('fishtalk_data', {}), dict)
        else {}
    )

    merged_folder = merged.setdefault('folder_config', {}).setdefault('fishtalk_data', {})
    max_order = max([v.get('order', 0) for v in merged_folder.values()] + [0])
    for name, state in saved_fish_folder.items():
        clean_name = _sanitize_folder_name(name)
        if not clean_name:
            continue
        max_order += 1
        merged_folder[clean_name] = {
            'order': _to_int(state.get('order', max_order), max_order),
            'visible': bool(state.get('visible', True)),
        }

    _ensure_folder_consistency(merged)
    return merged


def _sync_kpi_thresholds_to_db(db_manager):
    kpi_config = st.session_state.get('param_config', {}).get('kpi_config', {})
    if not kpi_config:
        return
    try:
        for _, cfg in kpi_config.items():
            db_manager.con.execute(
                "UPDATE kpi_thresholds SET menor_a = ? WHERE tipo_kpi = ? AND departamento = ?",
                [cfg['umbral'], cfg['tipo_kpi'], cfg['departamento']],
            )
    except Exception:
        pass


def _init_param_config(db_manager, force_reload=False):
    if force_reload or 'param_config' not in st.session_state:
        default_cfg = _build_default_param_config(db_manager)
        saved_cfg = db_manager.load_app_setting(CONFIG_STORE_KEY)
        st.session_state.param_config = _merge_saved_config(default_cfg, saved_cfg)
        return

    default_cfg = _build_default_param_config(db_manager)
    session_cfg = st.session_state.get('param_config', {})
    st.session_state.param_config = _merge_saved_config(default_cfg, session_cfg)


def ensure_runtime_config(db_manager):
    if not st.session_state.get('_param_config_runtime_initialized', False):
        _init_param_config(db_manager, force_reload=True)
        st.session_state._param_config_runtime_initialized = True


def _persist_full_config(db_manager):
    _ensure_folder_consistency(st.session_state.param_config)
    _sync_kpi_thresholds_to_db(db_manager)
    return db_manager.save_app_setting(CONFIG_STORE_KEY, st.session_state.param_config)


def render_config_tab(db_manager):
    _init_param_config(db_manager)

    st.header("Configuracion de Parametros")
    st.caption("Edita alias, rangos y carpetas. Usa 'Guardar cambios' para persistir en la base.")

    _render_folder_manager()
    st.markdown("---")

    _render_table_config("Produccion (Excel Pesado)", "fishtalk_data")
    st.markdown("---")

    _render_table_config("Mediciones (Ambiental)", "mediciones_data")
    st.markdown("---")

    _render_kpi_config()
    st.markdown("---")

    if st.button("Guardar cambios", type="primary", key="save_param_config_btn", use_container_width=True):
        ok = _persist_full_config(db_manager)
        if ok:
            st.cache_data.clear()
            st.success("Cambios guardados correctamente.")
        else:
            st.error("No se pudieron guardar los cambios en la base.")


def _render_folder_manager():
    st.subheader("Carpetas del Sidebar")
    st.caption("Crea, elimina, oculta y ordena carpetas. Variables de carpetas eliminadas pasan a 'Otras Variables'.")

    folder_cfg = st.session_state.param_config.setdefault('folder_config', {}).setdefault('fishtalk_data', {})
    fish_cfg = st.session_state.param_config.get('fishtalk_data', {})

    c1, c2 = st.columns([4, 1])
    with c1:
        new_folder = st.text_input("Nueva carpeta", key="new_sidebar_folder_name", placeholder="Ej: Calidad Agua")
    with c2:
        add_folder = st.button("Crear", key="create_sidebar_folder_btn", use_container_width=True)

    if add_folder:
        clean = _sanitize_folder_name(new_folder)
        if not clean:
            st.warning("Ingresa un nombre valido para la carpeta.")
        elif clean in folder_cfg:
            st.info("Esa carpeta ya existe.")
        else:
            max_order = max([v.get('order', 0) for v in folder_cfg.values()] + [0])
            folder_cfg[clean] = {'order': max_order + 1, 'visible': True}
            st.session_state.new_sidebar_folder_name = ""
            st.rerun()

    rows = []
    ordered_names = [
        name for name, _ in sorted(folder_cfg.items(), key=lambda kv: _to_int(kv[1].get('order', 999999), 999999))
    ]
    for name in ordered_names:
        vars_in = [
            col for col, cfg in fish_cfg.items()
            if _sanitize_folder_name(cfg.get('grupo', '')) == name
        ]
        vars_in = sorted(vars_in, key=lambda c: _to_int(fish_cfg[c].get('orden', 999999), 999999))
        preview = ', '.join(vars_in[:6])
        if len(vars_in) > 6:
            preview += ', ...'

        rows.append({
            'Orden': _to_int(folder_cfg[name].get('order', 999999), 999999),
            'Carpeta': name,
            'Visible': bool(folder_cfg[name].get('visible', True)),
            'Eliminar': False,
            'Variables': preview,
        })

    if rows:
        df = pd.DataFrame(rows)
        edited = st.data_editor(
            df,
            column_config={
                'Orden': st.column_config.NumberColumn('Orden', width='small', step=1),
                'Carpeta': st.column_config.TextColumn('Carpeta', disabled=True, width='medium'),
                'Visible': st.column_config.CheckboxColumn('Visible', width='small', default=True),
                'Eliminar': st.column_config.CheckboxColumn('Eliminar', width='small', default=False),
                'Variables': st.column_config.TextColumn('Variables', disabled=True, width='large'),
            },
            hide_index=True,
            use_container_width=True,
            num_rows='fixed',
            key='folder_manager_editor',
        )

        if edited is not None:
            to_delete = []
            for _, row in edited.iterrows():
                name = row['Carpeta']
                if name not in folder_cfg:
                    continue

                folder_cfg[name]['order'] = _to_int(row['Orden'], folder_cfg[name].get('order', 999999))
                folder_cfg[name]['visible'] = bool(row['Visible'])
                delete_mark = str(row['Eliminar']).strip().lower() in ('true', '1', 'yes')
                if delete_mark and name != 'Otras Variables':
                    to_delete.append(name)

            if to_delete:
                for name in to_delete:
                    folder_cfg.pop(name, None)
                    for _, cfg in fish_cfg.items():
                        if _sanitize_folder_name(cfg.get('grupo', '')) == name:
                            cfg['grupo'] = 'Otras Variables'

            _ensure_folder_consistency(st.session_state.param_config)

    with st.expander("Ver variables por carpeta", expanded=False):
        folder_cfg = st.session_state.param_config.get('folder_config', {}).get('fishtalk_data', {})
        ordered_names = [
            name for name, _ in sorted(folder_cfg.items(), key=lambda kv: _to_int(kv[1].get('order', 999999), 999999))
        ]
        for name in ordered_names:
            vars_in = [
                (col, cfg) for col, cfg in fish_cfg.items()
                if _sanitize_folder_name(cfg.get('grupo', '')) == name
            ]
            vars_in = sorted(vars_in, key=lambda kv: _to_int(kv[1].get('orden', 999999), 999999))
            if not vars_in:
                continue
            st.markdown(f"**{name}**")
            st.dataframe(
                pd.DataFrame([
                    {'Variable': col, 'Alias': cfg.get('alias', col), 'Orden': _to_int(cfg.get('orden', 999999), 999999)}
                    for col, cfg in vars_in
                ]),
                use_container_width=True,
                hide_index=True,
            )


def _render_table_config(title: str, table_key: str):
    config = st.session_state.param_config.get(table_key, {})
    if not config:
        st.info(f"No hay variables numericas en {title}.")
        return

    st.subheader(title)

    folder_options = get_sidebar_group_order('fishtalk_data', include_hidden=True)
    if not folder_options:
        folder_options = list(DEFAULT_GROUPS.keys()) + DEFAULT_EXTRA_GROUPS

    rows = []
    for col_name, cfg in config.items():
        rows.append({
            'Visible': cfg.get('visible', True),
            'Variable Original': col_name,
            'Nombre Personalizado': cfg.get('alias', col_name),
            'Carpeta': cfg.get('grupo', '') if table_key == 'fishtalk_data' else '',
            'Orden': _to_int(cfg.get('orden', 999999), 999999),
            'Desde': cfg.get('min', cfg.get('data_min', 0.0)),
            'Hasta': cfg.get('max', cfg.get('data_max', 0.0)),
            'Min. Datos': cfg.get('data_min', 0.0),
            'Max. Datos': cfg.get('data_max', 0.0),
        })

    df = pd.DataFrame(rows)

    column_config = {
        'Visible': st.column_config.CheckboxColumn('Visible', width='small', default=True),
        'Variable Original': st.column_config.TextColumn('Variable Original', disabled=True, width='medium'),
        'Nombre Personalizado': st.column_config.TextColumn('Nombre Personalizado', width='medium'),
        'Carpeta': st.column_config.TextColumn('Carpeta', disabled=True, width='medium') if table_key != 'fishtalk_data' else st.column_config.SelectboxColumn(
            'Carpeta',
            options=folder_options,
            width='medium',
            help='Carpeta donde aparecera la variable en el sidebar',
        ),
        'Orden': st.column_config.NumberColumn('Orden', width='small', step=1),
        'Desde': st.column_config.NumberColumn('Desde', format='%.4f', width='small'),
        'Hasta': st.column_config.NumberColumn('Hasta', format='%.4f', width='small'),
        'Min. Datos': st.column_config.NumberColumn('Min. Datos', disabled=True, format='%.4f', width='small'),
        'Max. Datos': st.column_config.NumberColumn('Max. Datos', disabled=True, format='%.4f', width='small'),
    }

    edited_df = st.data_editor(
        df,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        num_rows='fixed',
        key=f"config_editor_{table_key}",
    )

    if edited_df is not None:
        for _, row in edited_df.iterrows():
            col_name = row['Variable Original']
            if col_name not in st.session_state.param_config[table_key]:
                continue
            entry = st.session_state.param_config[table_key][col_name]
            entry['alias'] = str(row['Nombre Personalizado'])
            entry['min'] = _to_float(row['Desde'], entry.get('min', 0.0))
            entry['max'] = _to_float(row['Hasta'], entry.get('max', 0.0))
            entry['visible'] = bool(row['Visible'])
            entry['orden'] = _to_int(row['Orden'], entry.get('orden', 999999))

            if table_key == 'fishtalk_data':
                selected_folder = _sanitize_folder_name(str(row['Carpeta']))
                if selected_folder in folder_options:
                    entry['grupo'] = selected_folder


def _render_kpi_config():
    kpi_config = st.session_state.param_config.get('kpi_config', {})
    if not kpi_config:
        st.info("No hay KPIs cargados. Sube el archivo 'KPIs y Proyecciones por Batch'.")
        return

    st.subheader("KPIs (Umbrales)")
    st.caption("Edita los umbrales. Se guardan al presionar 'Guardar cambios'.")

    rows = []
    for _, cfg in kpi_config.items():
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
            'Visible': st.column_config.CheckboxColumn('Visible', width='small', default=True),
            'Tipo KPI': st.column_config.TextColumn('Tipo KPI', disabled=True, width='medium'),
            'Departamento': st.column_config.TextColumn('Departamento', disabled=True, width='small'),
            'Umbral (Menor a)': st.column_config.NumberColumn('Umbral (Menor a)', format='%.6f', width='small'),
            'Valor Original': st.column_config.NumberColumn('Valor Original', disabled=True, format='%.6f', width='small'),
        },
        use_container_width=True,
        hide_index=True,
        num_rows='fixed',
        key='config_editor_kpis',
    )

    if edited_df is not None:
        for _, row in edited_df.iterrows():
            key = f"{row['Tipo KPI']}|{row['Departamento']}"
            if key not in st.session_state.param_config['kpi_config']:
                continue
            entry = st.session_state.param_config['kpi_config'][key]
            entry['umbral'] = _to_float(row['Umbral (Menor a)'], entry['umbral'])
            entry['visible'] = bool(row['Visible'])


def get_alias_map(table_key: str = 'fishtalk_data') -> dict:
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {
        col: cfg.get('alias', col)
        for col, cfg in config.items()
        if cfg.get('alias', col) != col
    }


def get_range_filters(table_key: str = 'fishtalk_data') -> dict:
    config = st.session_state.get('param_config', {}).get(table_key, {})
    modified = {}
    for col, cfg in config.items():
        if cfg.get('min') != cfg.get('data_min') or cfg.get('max') != cfg.get('data_max'):
            modified[col] = (cfg.get('min'), cfg.get('max'))
    return modified


def get_kpi_config_thresholds() -> dict:
    kpi_config = st.session_state.get('param_config', {}).get('kpi_config', {})
    result = {}
    for _, cfg in kpi_config.items():
        if not cfg.get('visible', True):
            continue
        tipo = cfg['tipo_kpi']
        dept = cfg['departamento']
        result.setdefault(tipo, {})[dept] = cfg['umbral']
    return result


def get_hidden_variables(table_key: str = 'fishtalk_data') -> set:
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {col for col, cfg in config.items() if not cfg.get('visible', True)}


def get_variable_group_overrides(table_key: str = 'fishtalk_data') -> dict:
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {
        col: cfg.get('grupo', '')
        for col, cfg in config.items()
        if str(cfg.get('grupo', '')).strip()
    }


def get_variable_order_overrides(table_key: str = 'fishtalk_data') -> dict:
    config = st.session_state.get('param_config', {}).get(table_key, {})
    return {
        col: _to_int(cfg.get('orden', 999999), 999999)
        for col, cfg in config.items()
    }


def get_sidebar_group_order(table_key: str = 'fishtalk_data', include_hidden=False) -> list:
    folder_cfg = (
        st.session_state.get('param_config', {})
        .get('folder_config', {})
        .get(table_key, {})
    )
    if not isinstance(folder_cfg, dict) or not folder_cfg:
        return []

    ordered = sorted(folder_cfg.items(), key=lambda kv: _to_int(kv[1].get('order', 999999), 999999))
    names = []
    for name, state in ordered:
        if include_hidden or bool(state.get('visible', True)):
            names.append(name)
    return names
