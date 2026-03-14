import streamlit as st
import pandas as pd
import json
import datetime as dt
import copy
from src.filters import render_filters
from src.data_processing import load_and_clean_data
from src.db_manager import DBManager
from src.visualizations import create_main_chart
from src.config_params import render_config_tab, get_range_filters, get_alias_map, get_kpi_config_thresholds, ensure_runtime_config
from src.styles import inject_styles, inject_logo, show_loading_screen, hide_loading_screen, show_view_transition
import traceback
import unicodedata

try:
    from streamlit_plotly_events import plotly_events as _plotly_events
    PLOTLY_EVENTS_AVAILABLE = True
except Exception:
    _plotly_events = None
    PLOTLY_EVENTS_AVAILABLE = False


def safe_plotly_events(fig, key: str):
    if not PLOTLY_EVENTS_AVAILABLE or _plotly_events is None:
        return []
    return _plotly_events(
        fig,
        click_event=True,
        hover_event=False,
        select_event=False,
        override_width="100%",
        key=key,
    )


def _to_jsonable(value):
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, set):
        return sorted(_to_jsonable(v) for v in value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    try:
        if hasattr(value, 'item'):
            return value.item()
    except Exception:
        pass
    return value


def _cache_key(payload) -> str:
    return json.dumps(_to_jsonable(payload), ensure_ascii=True, sort_keys=True, separators=(',', ':'))


@st.cache_data(ttl=600, max_entries=64, show_spinner=False)
def _cached_mediciones_metadata(_db_manager, data_version: str):
    return _db_manager.get_mediciones_metadata()


@st.cache_data(ttl=600, max_entries=64, show_spinner=False)
def _cached_mediciones_date_range(_db_manager, data_version: str):
    return _db_manager.get_mediciones_date_range()


@st.cache_data(ttl=600, max_entries=64, show_spinner=False)
def _cached_kpi_thresholds(_db_manager, data_version: str):
    return _db_manager.get_kpi_thresholds()


@st.cache_data(ttl=600, max_entries=64, show_spinner=False)
def _cached_proyecciones_metadata(_db_manager, data_version: str):
    return _db_manager.get_proyecciones_metadata()


@st.cache_data(ttl=120, max_entries=8, show_spinner=False)
def _cached_filtered_data(_db_manager, data_version: str, filters_key: str):
    return _db_manager.get_filtered_data(json.loads(filters_key))


@st.cache_data(ttl=120, max_entries=16, show_spinner=False)
def _cached_mediciones_chart_data(_db_manager, data_version: str, filters_key: str):
    return _db_manager.get_mediciones_chart_data(json.loads(filters_key))


@st.cache_data(ttl=180, max_entries=16, show_spinner=False)
def _cached_proyecciones_data(_db_manager, data_version: str, request_key: str):
    req = json.loads(request_key)
    return _db_manager.get_proyecciones_data(
        batches=req.get('batches'),
        variables=req.get('variables'),
        date_range=req.get('date_range'),
    )

# --- Page Config ---
st.set_page_config(
    page_title="Cermaq Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize DB Manager
if 'db_manager' not in st.session_state:
    st.session_state.db_manager = DBManager()

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = st.session_state.db_manager.has_any_data()



# --- Config Dialog (modal overlay) ---
@st.dialog("⚙️ Configuración de Parámetros", width="large")
def show_config_dialog():
    render_config_tab(st.session_state.db_manager)

# --- Add Cards Dialog ---
@st.dialog("➕ Agregar Tarjetas", width="large")
def show_add_cards_dialog(hidden_cards):
    st.caption("Selecciona las métricas que deseas agregar al panel principal.")
    
    # Filter/Search
    search = st.text_input("🔍 Buscar...", placeholder="Nombre de lote, variable...", label_visibility="collapsed")
    
    filtered_hidden = hidden_cards
    if search:
        search = search.lower()
        filtered_hidden = [c for c in hidden_cards if search in c['series'].lower() or search in c['var'].lower()]
    
    if not filtered_hidden:
        st.info("No se encontraron tarjetas.")
        return

    # List items
    for c in filtered_hidden:
        c1, c2, c3 = st.columns([0.6, 0.3, 0.1])
        with c1:
            st.markdown(f"**{c['series']}**")
        with c2:
            st.caption(c['var'])
        with c3:
            if st.button("➕", key=f"add_{c['id']}", help="Agregar al tablero"):
                st.session_state.visible_cards.append(c['id'])
                st.rerun()
        st.markdown("---")

@st.dialog("👤 Seleccionar Perfil", width="small")
def show_profile_dialog():
    st.write("Selecciona el perfil de Dashboard que deseas visualizar:")
    
    # Available profiles
    profiles = {"maestro": "Maestro"}
    
    for key, label in profiles.items():
        # Highlight active profile
        btn_type = "primary" if st.session_state.get('current_profile') == key else "secondary"
        if st.button(label, key=f"btn_profile_{key}", type=btn_type, use_container_width=True):
            st.session_state.current_profile = key
            st.rerun()


def process_uploaded_files(uploaded_files, button_label: str, button_key: str):
    if not uploaded_files:
        return

    if st.button(button_label, type="primary", key=button_key):
        loading = show_loading_screen("Limpiando y consolidando datos...")
        pre_update_version = None
        try:
            kpi_files = []
            data_files = []
            med_files = []
            file_names = []

            for f in uploaded_files:
                fname = unicodedata.normalize('NFKD', f.name).encode('ascii', 'ignore').decode('ascii').lower()
                file_names.append(f.name)
                if 'kpi' in fname or 'proyecci' in fname:
                    kpi_files.append(f)
                elif 'medicion' in fname:
                    med_files.append(f)
                else:
                    data_files.append(f)

            # Safety snapshot before applying updates (keep latest 10 versions).
            if st.session_state.db_manager.has_any_data():
                pre_update_version = st.session_state.db_manager.create_data_snapshot(
                    reason='before_upload',
                    source_files=file_names,
                    keep_last=10,
                )

            if data_files:
                combined_df = load_and_clean_data(data_files)
                if not combined_df.empty:
                    st.session_state.db_manager.ingest_data(combined_df, table_name='fishtalk_data')

            for mf in med_files:
                st.session_state.db_manager.ingest_mediciones_data(mf)

            for kf in kpi_files:
                st.session_state.db_manager.ingest_kpis_proyecciones(kf)

            if 'param_config' in st.session_state:
                del st.session_state.param_config
            st.session_state._param_config_runtime_initialized = False

            st.cache_data.clear()
            st.session_state.applied_filters = None
            st.session_state.applied_filters_key = None

            st.session_state.data_loaded = st.session_state.db_manager.has_any_data()
            hide_loading_screen(loading)

            if st.session_state.data_loaded:
                if pre_update_version:
                    st.success(f"Datos procesados correctamente. Version previa guardada: {pre_update_version}")
                else:
                    st.success("Datos procesados correctamente.")
                st.rerun()
            else:
                st.warning("Los archivos no contenían datos válidos.")

        except Exception as e:
            rollback_msg = ""
            if pre_update_version:
                ok_restore, _ = st.session_state.db_manager.restore_data_version(
                    pre_update_version,
                    create_backup=False,
                    keep_last=10,
                )
                if ok_restore:
                    rollback_msg = f"\nSe restauró automáticamente la versión previa ({pre_update_version})."

            if loading:
                loading.empty()
            st.error(f"Error crítico al procesar: {str(e)}{rollback_msg}")
            with st.expander("Detalles técnicos"):
                st.text(traceback.format_exc())


def render_versions_manager():
    with st.sidebar.expander("🕘 Versiones de datos", expanded=False):
        versions = st.session_state.db_manager.list_data_versions(limit=10)
        if not versions:
            st.caption("Aún no hay versiones guardadas.")
            return

        labels = []
        mapping = {}
        for v in versions:
            ts = v.get('created_at')
            ts_txt = str(ts)[:19] if ts is not None else 'sin-fecha'
            reason = str(v.get('reason') or 'manual')
            vid = str(v.get('version_id'))
            label = f"{ts_txt} | {reason} | {vid}"
            labels.append(label)
            mapping[label] = vid

        selected_label = st.selectbox(
            "Selecciona versión",
            options=labels,
            key="version_restore_select",
            label_visibility="collapsed",
        )

        confirm_restore = st.checkbox("Confirmo restaurar esta versión", key="confirm_restore_version")
        if st.button("Restaurar versión", type="secondary", key="restore_version_btn", use_container_width=True):
            if not confirm_restore:
                st.warning("Activa la confirmación para restaurar.")
                return

            target_version = mapping.get(selected_label)
            if not target_version:
                st.warning("No se pudo resolver la versión seleccionada.")
                return

            loading = show_loading_screen("Restaurando versión de datos...")
            ok, backup_version = st.session_state.db_manager.restore_data_version(
                target_version,
                create_backup=True,
                keep_last=10,
            )
            hide_loading_screen(loading)

            if ok:
                if 'param_config' in st.session_state:
                    del st.session_state.param_config
                st.session_state._param_config_runtime_initialized = False
                st.cache_data.clear()
                st.session_state.applied_filters = None
                st.session_state.applied_filters_key = None
                st.session_state.data_loaded = st.session_state.db_manager.has_any_data()

                if backup_version:
                    st.success(f"Versión restaurada ({target_version}). Backup creado: {backup_version}")
                else:
                    st.success(f"Versión restaurada ({target_version}).")
                st.rerun()
            else:
                st.error("No se pudo restaurar la versión seleccionada.")

def main():
    inject_styles()
    db_status = st.session_state.db_manager.get_connection_status()
    data_version = _cache_key({
        'rev': db_status.get('data_revision', 0),
        'rows': db_status.get('rows', {}),
        'mode': db_status.get('mode', 'local'),
        'db': db_status.get('database', ':memory:'),
    })
    st.session_state.data_loaded = db_status.get('has_data', False)
    
    # Initialize View State
    if 'current_view' not in st.session_state:
        st.session_state.current_view = "Main"
    if 'current_profile' not in st.session_state:
        st.session_state.current_profile = "maestro"
    if not st.session_state.get('data_loaded') and st.session_state.db_manager.has_any_data():
        st.session_state.data_loaded = True

    ensure_runtime_config(st.session_state.db_manager)

    # --- Header ---
    if st.session_state.current_view == "Main":
        col_header, col_status, col_btn_dash, col_config = st.columns([3, 1, 0.6, 0.4])
        with col_header:
            inject_logo(dashboard_mode=False)
            
        with col_status:
            if 'data_loaded' in st.session_state and st.session_state.data_loaded:
                 st.markdown("🟢 <span style='color:#4ADE80; font-weight:600'>Datos Cargados</span>", unsafe_allow_html=True)
                 if db_status.get('mode') == 'motherduck':
                     st.caption("☁️ MotherDuck")
                 else:
                     st.caption("💻 Local")
                     if db_status.get('connection_error'):
                         st.caption("⚠️ Revisa Secrets de MotherDuck")
                         with st.expander("Detalle conexión", expanded=False):
                             st.code(str(db_status.get('connection_error')))
            else:
                 st.markdown("⚪ <span style='color:#A0AEC0'>Esperando datos</span>", unsafe_allow_html=True)
                 
        with col_btn_dash:
            if 'data_loaded' in st.session_state and st.session_state.data_loaded:
                if st.button("📋 Dashboard", key="toggle_view_btn", help="Cambiar a la vista del Dashboard"):
                    show_view_transition()
                    st.session_state.current_view = "Dashboard"
                    st.rerun()
                    
        with col_config:
            if 'data_loaded' in st.session_state and st.session_state.data_loaded:
                if st.button("⚙️", key="config_gear_btn", help="Configuración de Parámetros"):
                    show_config_dialog()
                    
    elif st.session_state.current_view == "Dashboard":
        # Slimmer header for dashboard: only compressed logo and navigation button
        col_header, col_spacer, col_btn_prof, col_btn_main = st.columns([2, 4, 1, 1])
        with col_header:
            inject_logo(dashboard_mode=True)
        with col_btn_prof:
            if st.button("👤 Perfiles", key="open_profiles_btn", help="Cambiar de perfil", use_container_width=True):
                show_profile_dialog()
        with col_btn_main:
            if st.button("⬅️", key="back_to_main_btn", help="Volver a los datos y gráficos", use_container_width=True):
                show_view_transition()
                st.session_state.current_view = "Main"
                st.rerun()

    if not PLOTLY_EVENTS_AVAILABLE:
        st.sidebar.warning("Modo interactivo de torta deshabilitado (falta streamlit-plotly-events).")
    
    if st.session_state.current_view == "Main":
        st.markdown("---")

        # State for filters
        filters = {}
        draft_filters = {}
        med_bounds = (None, None)
        kpi_thresholds = {}

        if 'applied_filters' not in st.session_state:
            st.session_state.applied_filters = None
        if 'applied_filters_key' not in st.session_state:
            st.session_state.applied_filters_key = None

        with st.sidebar:
            st.markdown('<div id="listo-btn-anchor"></div>', unsafe_allow_html=True)
            apply_clicked = st.button("✅ Listo", type="secondary", key="apply_filters_btn_v3", use_container_width=True)
            st.markdown("---")

        with st.sidebar.expander("🔄 Cargar / Actualizar datos", expanded=not st.session_state.data_loaded):
            st.caption("Sube nuevos archivos para actualizar la base persistente.")
            uploaded_files_update = st.file_uploader(
                "Seleccionar Archivos",
                type=['xlsx', 'xls', 'xml', 'html'],
                accept_multiple_files=True,
                label_visibility="collapsed",
                key="update_files_uploader"
            )
            process_uploaded_files(uploaded_files_update, "Procesar archivos", "process_update_files_btn")

        render_versions_manager()

        # Render filters regardless; if there is no data, they appear empty without blocking access.
        med_meta = _cached_mediciones_metadata(st.session_state.db_manager, data_version)
        med_bounds = _cached_mediciones_date_range(st.session_state.db_manager, data_version)
        kpi_thresholds = get_kpi_config_thresholds() or _cached_kpi_thresholds(st.session_state.db_manager, data_version)
        proj_meta = _cached_proyecciones_metadata(st.session_state.db_manager, data_version)
        draft_filters = render_filters(
            st.session_state.db_manager,
            mediciones_meta=med_meta,
            mediciones_date_bounds=med_bounds,
            kpi_thresholds=kpi_thresholds,
            proyecciones_meta=proj_meta,
        )

        draft_filters_key = _cache_key(draft_filters)
        applied_filters = st.session_state.get('applied_filters')
        applied_filters_key = st.session_state.get('applied_filters_key')

        if apply_clicked:
            st.session_state.applied_filters = copy.deepcopy(draft_filters)
            st.session_state.applied_filters_key = draft_filters_key
            st.rerun()

        if st.session_state.applied_filters is not None:
            filters = copy.deepcopy(st.session_state.applied_filters)
            has_pending = draft_filters_key != st.session_state.get('applied_filters_key')
            if has_pending:
                st.sidebar.warning("Tienes cambios sin aplicar. Presiona 'Listo'.")
        else:
            filters = {}

        # --- Main Content ---
        if 'data_loaded' in st.session_state and st.session_state.data_loaded:
            if not filters:
                st.info("Selecciona filtros en el panel lateral y presiona 'Listo' para consultar.")
                return

            # Parameter config ranges are visual-only (per variable), not SQL row filters.
            variable_ranges_main = get_range_filters('fishtalk_data')
            variable_ranges_med = get_range_filters('mediciones_data')
            alias_map = get_alias_map('fishtalk_data')
            alias_map_med = get_alias_map('mediciones_data')
            
            # Execute Query
            with st.spinner("Consultando..."):
                filtered_df = _cached_filtered_data(
                    st.session_state.db_manager,
                    data_version,
                    _cache_key(filters),
                )
            
            if not filtered_df.empty:
                # Rename columns for display
                display_df = filtered_df.rename(columns=alias_map)
                
                # 2. Controls & Main Chart
                comparison_mode = 'Overlay'
                x_mode = 'Date'
                unite_vars = False
                align_first = False
                
                # Initialize Session State for Measurement
                if 'measured_points' not in st.session_state:
                    st.session_state.measured_points = []
                if 'last_selection' not in st.session_state:
                    st.session_state.last_selection = None
                
                batches_selected = filters.get('batches', [])
                selected_vars = filters.get('variables', [])
                multi_batch = len(batches_selected) > 1
                multi_vars = len(selected_vars) > 1
                
                # --- Dynamic FCR Toggle Logic ---
                if 'fcr_view_mode' not in st.session_state:
                    st.session_state.fcr_view_mode = "Vista general"
                
                # Switch view mode logic
                # "Vista general" = shows 'FCR Económico Acumulado', hides 'Final FCR Económico'
                # "Vista individual" = shows 'Final FCR Económico', hides 'FCR Económico Acumulado'
                
                # Actual list of variables to use for charting:
                chart_vars = list(selected_vars)
                
                if st.session_state.fcr_view_mode == "Vista individual":
                    if "FCR Económico Acumulado" in chart_vars:
                        # Swap it
                        chart_vars.remove("FCR Económico Acumulado")
                        # Look for the proper name in the dataframe
                        fcr_col = next((c for c in filtered_df.columns if c.strip().lower() == 'final fcr económico' or c.strip().lower() == 'final fcr economico'), None)
                        if fcr_col and fcr_col not in chart_vars:
                            chart_vars.append(fcr_col)
                            
                    if "FCR Biológico Acumulado" in chart_vars:
                        chart_vars.remove("FCR Biológico Acumulado")
                        fcr_bio_col = next((c for c in filtered_df.columns if c.strip().lower() == 'final fcr biológico' or c.strip().lower() == 'final fcr biologico'), None)
                        if fcr_bio_col and fcr_bio_col not in chart_vars:
                            chart_vars.append(fcr_bio_col)
                            
                    if "GF3 Acumulado" in chart_vars:
                        chart_vars.remove("GF3 Acumulado")
                        gf3_col = next((c for c in filtered_df.columns if c.strip().lower() == 'final gf3'), None)
                        if gf3_col and gf3_col not in chart_vars:
                            chart_vars.append(gf3_col)
                            
                    if "SGR Acumulado" in chart_vars:
                        chart_vars.remove("SGR Acumulado")
                        sgr_col = next((c for c in filtered_df.columns if c.strip().lower() == 'final sgr'), None)
                        if sgr_col and sgr_col not in chart_vars:
                            chart_vars.append(sgr_col)
                            
                    if "SFR Acumulado" in chart_vars:
                        chart_vars.remove("SFR Acumulado")
                        sfr_col = next((c for c in filtered_df.columns if c.strip().lower() == 'final sfr'), None)
                        if sfr_col and sfr_col not in chart_vars:
                            chart_vars.append(sfr_col)
                            
                    if "% Mortalidad Acumulada" in chart_vars:
                        chart_vars.remove("% Mortalidad Acumulada")
                        mort_col = next((c for c in filtered_df.columns if c.strip() == 'Final Mortalidad, porcentaje'), None)
                        if mort_col and mort_col not in chart_vars:
                            chart_vars.append(mort_col)
                            
                    if "% Mortalidad diaria" in chart_vars:
                        chart_vars.remove("% Mortalidad diaria")
                        mort_pct_col = next((c for c in filtered_df.columns if 'mortalidad' in c.lower() and 'porcentaje' in c.lower() and 'per' in c.lower()), None)
                        if mort_pct_col and mort_pct_col not in chart_vars:
                            chart_vars.append(mort_pct_col)
                    
                    if "% Pérdida Acumulada" in chart_vars:
                        chart_vars.remove("% Pérdida Acumulada")
                        perd_col = next((c for c in filtered_df.columns if 'pérdida' in c.lower() and 'número' in c.lower() and 'período' in c.lower()), None)
                        if perd_col and perd_col not in chart_vars:
                            chart_vars.append(perd_col)
                    
                    if "% Eliminación Acumulada" in chart_vars:
                        chart_vars.remove("% Eliminación Acumulada")
                        elim_col = next((c for c in filtered_df.columns if 'eliminados' in c.lower() and 'número' in c.lower() and 'período' in c.lower()), None)
                        if elim_col and elim_col not in chart_vars:
                            chart_vars.append(elim_col)
                            
                    if "Pérdida diaria %" in chart_vars:
                        chart_vars.remove("Pérdida diaria %")
                        perd_col = next((c for c in filtered_df.columns if 'pérdida' in c.lower() and 'número' in c.lower() and 'período' in c.lower()), None)
                        if perd_col and perd_col not in chart_vars:
                            chart_vars.append(perd_col)
                            
                    if "Eliminación diaria %" in chart_vars:
                        chart_vars.remove("Eliminación diaria %")
                        elim_col = next((c for c in filtered_df.columns if 'eliminados' in c.lower() and 'número' in c.lower() and 'período' in c.lower()), None)
                        if elim_col and elim_col not in chart_vars:
                            chart_vars.append(elim_col)
                            
                    if "Peso promedio" in chart_vars:
                        chart_vars.remove("Peso promedio")
                        peso_col = next((c for c in filtered_df.columns if c.strip().lower() == 'final peso prom'), None)
                        if peso_col and peso_col not in chart_vars:
                            chart_vars.append(peso_col)
                
                # Control layout
                ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4, ctrl_col5 = st.columns([2, 1, 1, 1, 1])
                
                with ctrl_col1:
                    chart_type = st.radio(
                        "Tipo de Gráfico",
                        ['Líneas', 'Líneas + Marcadores', 'Barras', 'Área', 'Dispersión', 'Torta'],
                        horizontal=True,
                        index=0
                    )
                
                with ctrl_col2:
                    if multi_batch:
                        overlay_on = st.checkbox("🔀 Superponer", value=False, key="overlay_btn",
                                                help="Superponer lotes usando Días de Cultivo como eje X")
                        if overlay_on:
                            x_mode = 'Days'
                            comparison_mode = 'Overlay'
                            align_first = st.checkbox("📏 Desde 1er Reg.", value=False, help="Alinear inicio de todas las curvas a 0")
                 
                with ctrl_col3:
                    if multi_vars:
                        unite_vars = st.checkbox("🔗 Unir Variables", value=False, key="unite_vars_btn",
                                                help="Mostrar todas las variables en un solo gráfico")
                    else:
                        st.caption("")
                
                with ctrl_col4:
                    measure_mode = st.checkbox("📏 Medir", value=False, key="measure_btn", help="Selecciona puntos en el gráfico para ver diferencias")
                    if x_mode == 'Days' and not measure_mode:
                        st.info("Eje X: Días")
    
                with ctrl_col5:
                    pass
    
                # Chart + Stats
                if not chart_vars:
                    st.warning("Selecciona al menos una variable para visualizar.")
                else:
                    try:
                        if 'pie_view_mode' not in st.session_state:
                            st.session_state.pie_view_mode = "parents"

                        # --- Inject Causes Logic ---
                        actual_chart_vars = chart_vars.copy()
                        
                        # Detect trio specifically to rewrite variables depending on the state
                        is_trio = (
                            set(v.strip().lower() for v in actual_chart_vars) == 
                            {"% pérdida acumulada", "% eliminación acumulada", "% mortalidad acumulada"}
                        )
                        
                        if chart_type == 'Torta' and is_trio:
                            cause_names = [
                                'Embrionaria', 'Deforme Embrionaria', 'Micosis', 'Daño Mecánico Otros',
                                'Desadaptado', 'Deforme', 'Descompuesto', 'Aborto', 'Daño Mecánico',
                                'Sin causa Aparente', 'Maduro', 'Muestras', 'Operculo Corto',
                                'Rezagado', 'Nefrocalcinosis', 'Exofialosis', 'Daño Mecánico por Muestreo',
                            ]
                            causes_vars = [f"% Mortalidad {c} Acumulada" for c in cause_names]
                            
                            if st.session_state.pie_view_mode == "parents":
                                # Just keep Pérdida as it is requested
                                actual_chart_vars = ["% Pérdida Acumulada"]
                            elif st.session_state.pie_view_mode == "children":
                                actual_chart_vars = ["% Eliminación Acumulada", "% Mortalidad Acumulada"]
                            elif st.session_state.pie_view_mode == "causes":
                                actual_chart_vars = ["% Eliminación Acumulada"] + causes_vars
                        elif chart_type == 'Torta' and st.session_state.pie_view_mode == "causes" and not is_trio:
                            cause_names = [
                                'Embrionaria', 'Deforme Embrionaria', 'Micosis', 'Daño Mecánico Otros',
                                'Desadaptado', 'Deforme', 'Descompuesto', 'Aborto', 'Daño Mecánico',
                                'Sin causa Aparente', 'Maduro', 'Muestras', 'Operculo Corto',
                                'Rezagado', 'Nefrocalcinosis', 'Exofialosis', 'Daño Mecánico por Muestreo',
                            ]
                            is_daily = any('diaria' in v.lower() for v in actual_chart_vars if 'mortalidad' in v.lower())
                            cause_suffix = 'Diaria' if is_daily else 'Acumulada'
                            causes_vars = [f"% Mortalidad {c} {cause_suffix}" for c in cause_names]
                            
                            actual_chart_vars = [v for v in actual_chart_vars if "mortalidad" not in v.lower() or "causa" in v.lower()]

                        # --- Fetch projection data (only explicitly selected) ---
                        selected_proj_vars = filters.get('proyecciones_vars', [])
                        proj_df_for_chart = None
                        if selected_proj_vars:
                            proj_df_for_chart = _cached_proyecciones_data(
                                st.session_state.db_manager,
                                data_version,
                                _cache_key({
                                    'batches': filters.get('batches', []),
                                    'variables': selected_proj_vars,
                                    'date_range': filters.get('date_range'),
                                }),
                            )
                            if proj_df_for_chart is not None and proj_df_for_chart.empty:
                                proj_df_for_chart = None

                        main_uirevision = _cache_key({
                            'scope': 'main_chart',
                            'vars': sorted([str(v) for v in actual_chart_vars]),
                            'batches': sorted([str(b) for b in filters.get('batches', [])]),
                            'depts': sorted([str(d) for d in filters.get('depts', [])]),
                            'units': sorted([str(u) for u in filters.get('units', [])]),
                            'comparison_mode': comparison_mode,
                            'x_mode': x_mode,
                            'unite_vars': bool(unite_vars),
                            'independent_axes': False,
                            'granularity': filters.get('granularity', 'Día'),
                        })

                        fig = create_main_chart(
                            filtered_df, actual_chart_vars, comparison_mode, x_mode, chart_type, 
                            sum_units=filters.get('sum_units', False), 
                            avg_units=filters.get('avg_units', False), 
                            align_first=align_first, 
                            hover_mode='closest' if measure_mode else 'x unified',
                            highlight_points=st.session_state.measured_points if measure_mode else None,
                            unite_variables=unite_vars, 
                            rename_map=alias_map,
                            pie_view_mode=st.session_state.pie_view_mode,
                            kpi_thresholds=kpi_thresholds if filters.get('active_kpis') else None,
                            active_kpis=filters.get('active_kpis', []),
                            proyecciones_df=proj_df_for_chart,
                            variable_ranges=variable_ranges_main,
                            uirevision_key=main_uirevision,
                        )
                        
                        st.markdown('<div style="border-radius: 12px; overflow: hidden; border: 1px solid #2B303B;">', unsafe_allow_html=True)
                        
                        # Interactivity
                        if measure_mode:
                            # Capture selection events for Measurement Mode
                            selection_data = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode=["points"], key="main_chart_measure")
                            
                            # Process Selection Logic (Stateful)
                            if selection_data != st.session_state.last_selection:
                                st.session_state.last_selection = selection_data
                                
                                if selection_data and selection_data.get('selection') and selection_data['selection']['points']:
                                    new_point = selection_data['selection']['points'][0]
                                    if len(st.session_state.measured_points) >= 2:
                                        st.session_state.measured_points.pop(0)
                                    st.session_state.measured_points.append(new_point)
                                    st.rerun()
                        else:
                            # Standard or Pie Interactive Mode
                            if chart_type == 'Torta':
                                var_lower_map = {v: v.lower() for v in chart_vars}
                                loss_vars = [v for v in chart_vars if "pérdida" in var_lower_map[v]]
                                mort_vars = [v for v in chart_vars if "mortalidad" in var_lower_map[v] and "causa" not in var_lower_map[v]]
                                elim_vars = [v for v in chart_vars if "eliminación" in var_lower_map[v]]
                                
                                has_hierarchy = len(loss_vars) > 0 and (len(mort_vars) > 0 or len(elim_vars) > 0)
                                has_mort = len(mort_vars) > 0
                                
                                # Detect trio
                                is_trio = (
                                    "% pérdida acumulada" in var_lower_map and
                                    "% eliminación acumulada" in var_lower_map and
                                    "% mortalidad acumulada" in var_lower_map and
                                    len(chart_vars) == 3
                                )
                                
                                is_interactive = has_hierarchy or has_mort or is_trio
                                
                                if is_interactive:
                                    # State transitions: parents -> children -> causes -> parents
                                    # If no hierarchy (only mort), toggle: parents -> causes -> parents
                                    if PLOTLY_EVENTS_AVAILABLE:
                                        clicked = safe_plotly_events(fig, key=f"pie_interactive_{st.session_state.pie_view_mode}")
                                    else:
                                        st.plotly_chart(fig, use_container_width=True, key=f"pie_interactive_fallback_{st.session_state.pie_view_mode}")
                                        clicked = []
                                    
                                    if clicked:
                                        if is_trio or has_hierarchy:
                                            if st.session_state.pie_view_mode == "parents":
                                                st.session_state.pie_view_mode = "children"
                                            elif st.session_state.pie_view_mode == "children":
                                                st.session_state.pie_view_mode = "causes"
                                            else:
                                                st.session_state.pie_view_mode = "parents"
                                        elif has_mort:
                                            if st.session_state.pie_view_mode == "parents":
                                                st.session_state.pie_view_mode = "causes"
                                            else:
                                                st.session_state.pie_view_mode = "parents"
                                        st.rerun()
                                else:
                                    st.plotly_chart(fig, use_container_width=True, key="pie_static")
                            else:
                                st.plotly_chart(fig, use_container_width=True, key="main_chart_static")
                                
                            # Reset measurement state when mode is toggled off
                            if st.session_state.measured_points:
                                st.session_state.measured_points = []
                                st.session_state.last_selection = None
                            
                        st.markdown('</div>', unsafe_allow_html=True)
    
                        # Measurement Results Display (Based on Persisted State)
                        if measure_mode:
                            if len(st.session_state.measured_points) == 2:
                                p1 = st.session_state.measured_points[0]
                                p2 = st.session_state.measured_points[1]
                                
                                try:
                                    # Handle datetime strings for X safely
                                    if type(p1.get('x')) is str and type(p2.get('x')) is str:
                                        try:
                                            d1 = pd.to_datetime(p1['x'])
                                            d2 = pd.to_datetime(p2['x'])
                                            dx = f"{(d2 - d1).days} días"
                                        except:
                                            dx = f"{p1.get('x')} → {p2.get('x')}"
                                    else:
                                        dx = f"{p2.get('x', 0) - p1.get('x', 0):,.2f}"
                                        
                                    y1 = float(p1.get('y', 0))
                                    y2 = float(p2.get('y', 0))
                                    dy = y2 - y1
                                    pct_change = (dy / y1 * 100) if y1 != 0 else 0
                                    
                                    st.markdown(f"""
                                    <div style="background:linear-gradient(145deg, rgba(78, 205, 196, 0.1), rgba(43, 48, 59, 0.4)); border:1px solid #4ECDC4; border-radius:12px; padding:16px; margin-bottom:16px; box-shadow:0 4px 15px rgba(0,0,0,0.2);">
                                        <h4 style="margin-top:0; color:#4ECDC4; margin-bottom:12px; font-weight:600;">📏 Diferencia de Medición</h4>
                                        <div style="display:flex; justify-content:space-between; flex-wrap:wrap; gap:12px;">
                                            <div style="background:rgba(0,0,0,0.2); padding:8px 12px; border-radius:8px;">
                                                <div style="font-size:0.75rem; color:rgba(255,255,255,0.6);">PUNTO 1</div>
                                                <div style="font-family:monospace; font-size:0.95rem;">X: {p1.get('x')} <br>Y: <b>{y1:,.2f}</b></div>
                                            </div>
                                            <div style="background:rgba(0,0,0,0.2); padding:8px 12px; border-radius:8px;">
                                                <div style="font-size:0.75rem; color:rgba(255,255,255,0.6);">PUNTO 2</div>
                                                <div style="font-family:monospace; font-size:0.95rem;">X: {p2.get('x')} <br>Y: <b>{y2:,.2f}</b></div>
                                            </div>
                                            <div style="background:rgba(0,0,0,0.2); padding:8px 12px; border-radius:8px; border-bottom:2px solid {'#FF6B6B' if pct_change < 0 else '#4ECDC4'};">
                                                <div style="font-size:0.75rem; color:rgba(255,255,255,0.6);">RESULTADO (Δ)</div>
                                                <div style="font-family:monospace; font-size:1.05rem;">
                                                    ΔX: {dx} <br>
                                                    ΔY: <b>{dy:,.2f}</b> <span style="color:{'#FF6B6B' if pct_change < 0 else '#4ECDC4'}">({pct_change:+.2f}%)</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                except Exception as e:
                                    st.error(f"Error procesando medición: {e}")
                            else:
                                st.info(f"Selecciona {2 - len(st.session_state.measured_points)} punto(s) más para medir.")
                        
                        # --- STATS GRID SYSTEM ---
                        st.markdown("###")
                        
                        # 1. Helper to calculate ALL stats
                        # Resolve series column (same logic as before)
                        def _find_col(name, df_cols):
                            for c in df_cols:
                                if c.lower() == name.lower(): return c
                            for c in df_cols:
                                if name.lower() in c.lower(): return c
                            return None
                        
                        lote_c = _find_col('lote', filtered_df.columns) or _find_col('batch', filtered_df.columns)
                        unit_c = _find_col('unidad', filtered_df.columns) or _find_col('unit', filtered_df.columns) or _find_col('jaula', filtered_df.columns)
                        dept_c = next((c for c in filtered_df.columns if 'departamento' in c.lower() or 'depto' in c.lower()), None)
                        days_c = _find_col('final days since first input', filtered_df.columns) or _find_col('primer ingreso', filtered_df.columns) or _find_col('days', filtered_df.columns)
                        date_c = _find_col('final fecha', filtered_df.columns) or _find_col('fecha', filtered_df.columns) or _find_col('date', filtered_df.columns)
                        
                        # Build series key (matching chart logic)
                        work_df = filtered_df.copy()
                        sum_on = filters.get('sum_units', False)
                        if sum_on:
                            if lote_c and dept_c: work_df['_series'] = work_df[lote_c].astype(str) + ' - ' + work_df[dept_c].astype(str)
                            elif lote_c: work_df['_series'] = work_df[lote_c].astype(str)
                            else: work_df['_series'] = 'Total'
                        else:
                            if lote_c and dept_c and unit_c: work_df['_series'] = work_df[lote_c].astype(str) + ' - ' + work_df[dept_c].astype(str) + ' - ' + work_df[unit_c].astype(str)
                            elif lote_c and unit_c: work_df['_series'] = work_df[lote_c].astype(str) + ' - ' + work_df[unit_c].astype(str)
                            elif lote_c: work_df['_series'] = work_df[lote_c].astype(str)
                            else: work_df['_series'] = 'Total'
                        
                        df_cols_lower = {c.lower(): c for c in filtered_df.columns}
                        
                        # Collect all potential cards
                        all_cards = []
                        for var in selected_vars:
                             actual_col = None
                             if var in filtered_df.columns: actual_col = var
                             elif var.lower() in df_cols_lower: actual_col = df_cols_lower[var.lower()]
                             else: actual_col = _find_col(var, filtered_df.columns)
                             
                             if not actual_col or not pd.api.types.is_numeric_dtype(filtered_df[actual_col]): continue
                             
                             display_var = alias_map.get(var, var)
                             
                             for series_name, grp in work_df.groupby('_series', sort=True):
                                 valid = grp[grp[actual_col].notna()]
                                 if valid.empty: continue
                                 
                                 v_min = valid[actual_col].min()
                                 v_max = valid[actual_col].max()
                                 v_avg = valid[actual_col].mean()
                                 
                                 # Hover Tooltip logic
                                 min_row = valid.loc[valid[actual_col].idxmin()]
                                 max_row = valid.loc[valid[actual_col].idxmax()]
                                 def _tooltip(row, label):
                                     parts = [f"{label}: {row[actual_col]:,.2f}"]
                                     if date_c and date_c in row.index:
                                         try: parts.append(f"Fecha: {pd.to_datetime(row[date_c]).strftime('%d/%m/%Y')}")
                                         except: parts.append(f"Fecha: {row[date_c]}")
                                     if days_c and days_c in row.index:
                                         try: parts.append(f"Día cultivo: {int(row[days_c])}")
                                         except: parts.append(f"Día cultivo: {row[days_c]}")
                                     if lote_c and lote_c in row.index: parts.append(f"Lote: {row[lote_c]}")
                                     if unit_c and unit_c in row.index: parts.append(f"Unidad: {row[unit_c]}")
                                     return '&#10;'.join(parts)

                                 card_data = {
                                     'id': f"{series_name} | {var}",
                                     'series': series_name,
                                     'var': display_var,
                                     'min': v_min,
                                     'max': v_max,
                                     'avg': v_avg,
                                     'tip_min': _tooltip(min_row, 'Mínimo'),
                                     'tip_max': _tooltip(max_row, 'Máximo'),
                                     'tip_avg': f"Promedio: {v_avg:,.2f}&#10;N registros: {len(valid)}"
                                 }
                                 all_cards.append(card_data)
    
                        # 2. State Management
                        if 'visible_cards' not in st.session_state:
                            # Initial load: take first 6
                            st.session_state.visible_cards = [c['id'] for c in all_cards[:6]]
                        else:
                            # Prune stale IDs (columns changed, etc)
                            current_ids = set(c['id'] for c in all_cards)
                            st.session_state.visible_cards = [id for id in st.session_state.visible_cards if id in current_ids]
                            # If empty but we have cards, add top 6 again (optional, dependent on UX pref. Let's keep empty if user removed all?)
                            # Actually if user changes variables, specific IDs change. 
                            # If list becomes empty due to pruning, re-populate.
                            if not st.session_state.visible_cards and all_cards:
                                 st.session_state.visible_cards = [c['id'] for c in all_cards[:6]]
    
                        # 3. Render Grid
                        visible_data = [c for c in all_cards if c['id'] in st.session_state.visible_cards]
                        hidden_data = [c for c in all_cards if c['id'] not in st.session_state.visible_cards]
                        
                        if visible_data:
                            # Grid Layout: 3 cards per row
                            cols_per_row = 3
                            for i in range(0, len(visible_data), cols_per_row):
                                row_cards = visible_data[i: i + cols_per_row]
                                cols = st.columns(cols_per_row)
                                for idx, cd in enumerate(row_cards):
                                    with cols[idx]:
                                        st.markdown('<div class="glass-card-zone">', unsafe_allow_html=True)
                                        # Card as single HTML block — NO indentation (Streamlit treats 4+ spaces as code block)
                                        card_html = f"""<div style="background:linear-gradient(160deg, rgba(255,255,255,0.07) 0%, rgba(255,255,255,0.02) 40%, rgba(0,0,0,0.3) 100%); border:1px solid rgba(255,255,255,0.12); border-radius:18px; padding:16px 20px 14px; box-shadow:0 8px 32px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.06); backdrop-filter:blur(40px) saturate(1.2); -webkit-backdrop-filter:blur(40px) saturate(1.2); position:relative; overflow:hidden;">
    <div style="position:absolute; top:0; left:0; right:0; height:1px; background:linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.15) 40%, rgba(255,255,255,0.15) 60%, transparent 100%);"></div>
    <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:14px;">
    <div style="overflow:hidden; flex:1; margin-right:8px;">
    <div style="font-family:'Inter','SF Pro Display',system-ui,sans-serif; font-size:0.78rem; font-weight:600; color:rgba(255,255,255,0.92); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="{cd['series']}">{cd['series']}</div>
    <div style="font-family:'Inter',sans-serif; font-size:0.65rem; color:rgba(255,255,255,0.35); font-weight:400; margin-top:2px; letter-spacing:0.3px;">{cd['var']}</div>
    </div>
    </div>
    <div style="height:1px; background:linear-gradient(90deg, transparent, rgba(255,255,255,0.08), transparent); margin-bottom:14px;"></div>
    <div style="display:flex; justify-content:space-between; gap:8px;">
    <div style="flex:1; cursor:help;" title="{cd['tip_min']}">
    <div style="font-family:'Inter',sans-serif; font-size:0.55rem; font-weight:600; color:rgba(255,255,255,0.25); text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">Mín</div>
    <div style="font-family:'SF Mono','Menlo','Roboto Mono',monospace; font-size:0.88rem; font-weight:500; color:rgba(255,255,255,0.85); letter-spacing:-0.3px;">{cd['min']:,.2f}</div>
    </div>
    <div style="flex:1; text-align:center; cursor:help; border-left:1px solid rgba(255,255,255,0.05); border-right:1px solid rgba(255,255,255,0.05); padding:0 4px;" title="{cd['tip_avg']}">
    <div style="font-family:'Inter',sans-serif; font-size:0.55rem; font-weight:600; color:rgba(255,255,255,0.25); text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">Prom</div>
    <div style="font-family:'SF Mono','Menlo','Roboto Mono',monospace; font-size:0.88rem; font-weight:500; color:rgba(255,255,255,0.85); letter-spacing:-0.3px;">{cd['avg']:,.2f}</div>
    </div>
    <div style="flex:1; text-align:right; cursor:help;" title="{cd['tip_max']}">
    <div style="font-family:'Inter',sans-serif; font-size:0.55rem; font-weight:600; color:rgba(255,255,255,0.25); text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">Máx</div>
    <div style="font-family:'SF Mono','Menlo','Roboto Mono',monospace; font-size:0.88rem; font-weight:500; color:rgba(255,255,255,0.85); letter-spacing:-0.3px;">{cd['max']:,.2f}</div>
    </div>
    </div>
    </div>"""
                                        st.markdown(card_html, unsafe_allow_html=True)
                                        # Tiny remove button
                                        if st.button("✕", key=f"rem_{cd['id']}", help="Ocultar tarjeta"):
                                            st.session_state.visible_cards.remove(cd['id'])
                                            st.rerun()
                                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        # 4. "Add More" Button
                        if hidden_data:
                            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
                            btn_cols = st.columns([1, 2, 1])
                            with btn_cols[1]:
                                 if st.button(f"➕ Ver más tarjetas ({len(hidden_data)} ocultas)", use_container_width=True):
                                      show_add_cards_dialog(hidden_data)
                        
                    except Exception as e:
                        st.error(f"Error al generar gráfico: {str(e)}")
    
                # --- 7. Visualizations (Mediciones Chart - New) ---
                if filters.get('mediciones_vars'):
                    st.markdown("### 🧪 Mediciones")
                    
                    # Chart controls row
                    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 0.9, 1])
                    with ctrl_col1:
                        med_chart_type = st.radio(
                            "Tipo de Gráfico (Mediciones)",
                            ['Líneas', 'Líneas + Marcadores', 'Barras', 'Área', 'Dispersión', 'Torta'],
                            horizontal=True,
                            index=0,
                            key="med_chart_type"
                        )
                    with ctrl_col2:
                        if filters.get('mediciones_avg', False):
                            st.caption("📊 Promediar: Activo")
                        else:
                            st.caption("📊 Promediar: Inactivo")
                    with ctrl_col3:
                        med_unir = st.checkbox("🔗 Unir variables", value=False, key="med_unir_vars",
                                               help="Muestra todas las variables en un solo gráfico")
                        med_axes = False
                        if med_unir:
                            med_axes = st.checkbox("🎚️ Ejes independientes", value=True, key="med_indep_axes",
                                                  help="Cada variable tendrá su propia escala Y")
                    
                    med_query_filters = {
                        'mediciones_places': filters.get('mediciones_places', []),
                        'mediciones_vars': filters.get('mediciones_vars', []),
                        'mediciones_date_range': filters.get('mediciones_date_range', []),
                        'mediciones_avg': filters.get('mediciones_avg', False),
                    }
                    med_df = _cached_mediciones_chart_data(
                        st.session_state.db_manager,
                        data_version,
                        _cache_key(med_query_filters),
                    )
                    
                    if not med_df.empty:
                        med_uirevision = _cache_key({
                            'scope': 'med_chart',
                            'vars': sorted([str(v) for v in filters.get('mediciones_vars', [])]),
                            'places': sorted([str(p) for p in filters.get('mediciones_places', [])]),
                            'unite': bool(med_unir),
                            'independent_axes': bool(med_axes),
                        })

                        fig_med = create_main_chart(
                            med_df, 
                            filters['mediciones_vars'], 
                            batch_comparison_mode='Overlay', 
                            x_axis_mode='Date', 
                            chart_type=med_chart_type,
                            hover_mode='closest',
                            unite_variables=med_unir,
                            independent_axes=med_axes,
                            rename_map=alias_map_med,
                            highlight_points=st.session_state.measured_points if measure_mode else None,
                            variable_ranges=variable_ranges_med,
                            uirevision_key=med_uirevision,
                        )
                        
                        if measure_mode and med_chart_type != 'Torta':
                            # Build unique keys to separate state from the main chart
                            sel_key = "med_chart_measure"
                            selection_data = st.plotly_chart(fig_med, use_container_width=True, on_select="rerun", selection_mode=["points"], key=sel_key)
                            
                            # Process Selection Logic
                            if selection_data != st.session_state.get('last_selection_med'):
                                st.session_state.last_selection_med = selection_data
                                if selection_data and selection_data.get('selection') and selection_data['selection']['points']:
                                    new_point = selection_data['selection']['points'][0]
                                    if len(st.session_state.measured_points) >= 2:
                                        st.session_state.measured_points.pop(0)
                                    st.session_state.measured_points.append(new_point)
                                    st.rerun()
                                    
                            # Render Delta Card
                            if len(st.session_state.measured_points) == 2:
                                p1 = st.session_state.measured_points[0]
                                p2 = st.session_state.measured_points[1]
                                
                                try:
                                    # Handle datetime strings for X safely
                                    if type(p1.get('x')) is str and type(p2.get('x')) is str:
                                        try:
                                            d1 = pd.to_datetime(p1['x'])
                                            d2 = pd.to_datetime(p2['x'])
                                            dx = f"{(d2 - d1).days} días"
                                        except:
                                            dx = f"{p1.get('x')} → {p2.get('x')}"
                                    else:
                                        dx = f"{p2.get('x', 0) - p1.get('x', 0):,.2f}"
                                        
                                    y1 = float(p1.get('y', 0))
                                    y2 = float(p2.get('y', 0))
                                    dy = y2 - y1
                                    pct_change = (dy / y1 * 100) if y1 != 0 else 0
                                    
                                    st.markdown(f"""
                                    <div style="background:linear-gradient(145deg, rgba(78, 205, 196, 0.1), rgba(43, 48, 59, 0.4)); border:1px solid #4ECDC4; border-radius:12px; padding:16px; margin-bottom:16px; box-shadow:0 4px 15px rgba(0,0,0,0.2);">
                                        <h4 style="margin-top:0; color:#4ECDC4; margin-bottom:12px; font-weight:600;">📏 Diferencia de Medición (Mediciones)</h4>
                                        <div style="display:flex; justify-content:space-between; flex-wrap:wrap; gap:12px;">
                                            <div style="background:rgba(0,0,0,0.2); padding:8px 12px; border-radius:8px;">
                                                <div style="font-size:0.75rem; color:rgba(255,255,255,0.6);">PUNTO 1</div>
                                                <div style="font-family:monospace; font-size:0.95rem;">X: {p1.get('x')} <br>Y: <b>{y1:,.2f}</b></div>
                                            </div>
                                            <div style="background:rgba(0,0,0,0.2); padding:8px 12px; border-radius:8px;">
                                                <div style="font-size:0.75rem; color:rgba(255,255,255,0.6);">PUNTO 2</div>
                                                <div style="font-family:monospace; font-size:0.95rem;">X: {p2.get('x')} <br>Y: <b>{y2:,.2f}</b></div>
                                            </div>
                                            <div style="background:rgba(0,0,0,0.2); padding:8px 12px; border-radius:8px; border-bottom:2px solid {'#FF6B6B' if pct_change < 0 else '#4ECDC4'};">
                                                <div style="font-size:0.75rem; color:rgba(255,255,255,0.6);">RESULTADO (Δ)</div>
                                                <div style="font-family:monospace; font-size:1.05rem;">
                                                    ΔX: {dx} <br>
                                                    ΔY: <b>{dy:,.2f}</b> <span style="color:{'#FF6B6B' if pct_change < 0 else '#4ECDC4'}">({pct_change:+.2f}%)</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                except Exception as e:
                                    st.error(f"Error procesando medición: {e}")
                            else:
                                st.info(f"Selecciona {2 - len(st.session_state.measured_points)} punto(s) más para medir.")
                        else:
                            st.plotly_chart(fig_med, use_container_width=True, key='med_chart_static')
                            if st.session_state.measured_points:
                                st.session_state.measured_points = []
                                st.session_state.last_selection_med = None
                    else:
                        st.info("No hay datos de medición para los filtros seleccionados.")
                        with st.expander("🕵️ Debug Info (Ayuda)", expanded=False):
                            st.write("Filtros Activos:", filters)
                            st.write("Rango Fechas (DB):", med_bounds)
                
                st.markdown("---")
    
                # 3. Data Table (Collapsible)
                with st.expander("Ver Datos Detallados", expanded=False):
                    if filters.get('sum_units'):
                         st.info("ℹ️ Modo 'Sumar Unidades' activo: Los datos están agrupados por Lote y Fecha.")
                    st.dataframe(display_df, use_container_width=True)

                with st.expander("Pasos de filtrado", expanded=False):
                    st.caption("Debug: muestra cómo se van filtrando los datos hasta el resultado final del gráfico.")
                    full_tables = st.checkbox(
                        "Mostrar tablas completas (puede ser pesado)",
                        value=False,
                        key="debug_filter_full_tables",
                    )
                    if st.button("Generar pasos de filtrado", key="gen_filter_steps_btn", use_container_width=True):
                        with st.spinner("Generando pasos de filtrado..."):
                            debug_steps = st.session_state.db_manager.get_filter_debug_steps(filters)

                        if not debug_steps:
                            st.info("No se pudieron generar pasos de filtrado para esta consulta.")
                        else:
                            for idx, step in enumerate(debug_steps, start=0):
                                step_name = step.get('name', f'Paso {idx}')
                                step_df = step.get('df', pd.DataFrame())
                                step_where = step.get('where', '')

                                st.markdown(f"**{step_name}**")
                                st.caption(f"Filas: {len(step_df)}")
                                if step_where:
                                    st.code(step_where)

                                if full_tables:
                                    st.dataframe(step_df, use_container_width=True)
                                else:
                                    preview_limit = 2000
                                    if len(step_df) > preview_limit:
                                        st.caption(f"Mostrando primeras {preview_limit} filas (activa 'completas' para ver todo).")
                                    st.dataframe(step_df.head(preview_limit), use_container_width=True)

                            # Visual-only ranges step (per variable), does not remove rows globally.
                            visual_df = filtered_df.copy()
                            applied_ranges = []
                            for var_name, (rmin, rmax) in (variable_ranges_main or {}).items():
                                if var_name in visual_df.columns:
                                    num = pd.to_numeric(visual_df[var_name], errors='coerce')
                                    visual_df[var_name] = visual_df[var_name].where((num >= float(rmin)) & (num <= float(rmax)))
                                    applied_ranges.append((var_name, rmin, rmax))

                            st.markdown("**Paso 7 - Rango visual por variable (solo gráfico)**")
                            st.caption(f"Filas: {len(visual_df)}")
                            if applied_ranges:
                                st.dataframe(
                                    pd.DataFrame(applied_ranges, columns=['Variable', 'Min', 'Max']),
                                    use_container_width=True,
                                    hide_index=True,
                                )
                            else:
                                st.caption("Sin rangos visuales activos.")

                            if full_tables:
                                st.dataframe(visual_df, use_container_width=True)
                            else:
                                preview_limit = 2000
                                if len(visual_df) > preview_limit:
                                    st.caption(f"Mostrando primeras {preview_limit} filas (activa 'completas' para ver todo).")
                                st.dataframe(visual_df.head(preview_limit), use_container_width=True)
    
            else:
                 st.warning("⚠️ No hay datos para la combinación de filtros seleccionada.")
                 
    elif st.session_state.current_view == "Dashboard":
        # Dashboard Content based on Profile
        profile = st.session_state.get('current_profile', 'maestro')
        
        if profile == "maestro":
            pass
        else:
            st.warning("Perfil no reconocido.")

    # Empty state when NO data loaded
    if 'data_loaded' not in st.session_state or not st.session_state.data_loaded:
        st.info("Aún no hay datos cargados en la base. Usa 'Cargar / Actualizar datos' en el panel lateral.")

if __name__ == "__main__":
    main()
