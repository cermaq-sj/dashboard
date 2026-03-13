import streamlit as st
import pandas as pd
from src.filters import render_filters
from src.data_processing import load_and_clean_data
from src.db_manager import DBManager
from src.calculations import calculate_kpis
from src.visualizations import create_main_chart
from src.config_params import render_config_tab, get_range_filters, get_alias_map, get_kpi_config_thresholds
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
        try:
            kpi_files = []
            data_files = []
            med_files = []

            for f in uploaded_files:
                fname = unicodedata.normalize('NFKD', f.name).encode('ascii', 'ignore').decode('ascii').lower()
                if 'kpi' in fname or 'proyecci' in fname:
                    kpi_files.append(f)
                elif 'medicion' in fname:
                    med_files.append(f)
                else:
                    data_files.append(f)

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

            st.session_state.data_loaded = st.session_state.db_manager.has_any_data()
            hide_loading_screen(loading)

            if st.session_state.data_loaded:
                st.success("Datos procesados correctamente.")
                st.rerun()
            else:
                st.warning("Los archivos no contenían datos válidos.")

        except Exception as e:
            if loading:
                loading.empty()
            st.error(f"Error crítico al procesar: {str(e)}")
            with st.expander("Detalles técnicos"):
                st.text(traceback.format_exc())

def main():
    inject_styles()
    db_status = st.session_state.db_manager.get_connection_status()
    st.session_state.data_loaded = db_status.get('has_data', False)
    
    # Initialize View State
    if 'current_view' not in st.session_state:
        st.session_state.current_view = "Main"
    if 'current_profile' not in st.session_state:
        st.session_state.current_profile = "maestro"
    if not st.session_state.get('data_loaded') and st.session_state.db_manager.has_any_data():
        st.session_state.data_loaded = True

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
        med_bounds = (None, None)
        kpi_thresholds = {}

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

        # Render filters regardless; if there is no data, they appear empty without blocking access.
        med_meta = st.session_state.db_manager.get_mediciones_metadata()
        med_bounds = st.session_state.db_manager.get_mediciones_date_range()
        kpi_thresholds = get_kpi_config_thresholds() or st.session_state.db_manager.get_kpi_thresholds()
        proj_meta = st.session_state.db_manager.get_proyecciones_metadata()
        filters = render_filters(
            st.session_state.db_manager,
            mediciones_meta=med_meta,
            mediciones_date_bounds=med_bounds,
            kpi_thresholds=kpi_thresholds,
            proyecciones_meta=proj_meta,
        )
    
        # --- Main Content ---
        if 'data_loaded' in st.session_state and st.session_state.data_loaded:
            
            # Inject param_ranges into filters
            filters['param_ranges'] = get_range_filters('fishtalk_data')
            filters['param_ranges_med'] = get_range_filters('mediciones_data')
            alias_map = get_alias_map('fishtalk_data')
            alias_map_med = get_alias_map('mediciones_data')
            
            # Execute Query
            with st.spinner("Consultando..."):
                filtered_df = st.session_state.db_manager.get_filtered_data(filters)
            
            if not filtered_df.empty:
                # Rename columns for display
                display_df = filtered_df.rename(columns=alias_map)
                
                # 1. KPI Cards
                kpis = calculate_kpis(filtered_df)
                if kpis:
                    cols = st.columns(len(kpis))
                    for i, kpi in enumerate(kpis):
                        cols[i].metric(kpi['label'], kpi['value'], kpi['unit'])
                else:
                    st.info("No hay suficientes datos para calcular KPIs.")
    
                st.markdown("###") # Spacer
                
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
                    else:
                        st.caption("Selecciona >1 lote para superponer")
                
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
                    elif not measure_mode:
                        st.caption("Eje X: Fecha")
    
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
                            proj_df_for_chart = st.session_state.db_manager.get_proyecciones_data(
                                batches=filters.get('batches', []),
                                variables=selected_proj_vars,
                                date_range=filters.get('date_range')
                            )
                            if proj_df_for_chart is not None and proj_df_for_chart.empty:
                                proj_df_for_chart = None

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
                    st.caption("Gráfico independiente (Rango de fechas propio)")
                    
                    # Chart controls row
                    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 1, 1])
                    with ctrl_col1:
                        med_chart_type = st.radio(
                            "Tipo de Gráfico (Mediciones)",
                            ['Líneas', 'Líneas + Marcadores', 'Barras', 'Área', 'Dispersión', 'Torta'],
                            horizontal=True,
                            index=0,
                            key="med_chart_type"
                        )
                    with ctrl_col2:
                        med_avg = st.checkbox("📊 Promediar", value=filters.get('mediciones_avg', False), key="med_avg_main",
                                             help="Promedia valores del mismo día/lugar")
                        if med_avg:
                            filters['mediciones_avg'] = True
                    with ctrl_col3:
                        med_unir = st.checkbox("🔗 Unir variables", value=False, key="med_unir_vars",
                                              help="Muestra todas las variables en un solo gráfico")
                        med_axes = False
                        if med_unir:
                            med_axes = st.checkbox("🎚️ Ejes independientes", value=True, key="med_indep_axes",
                                                  help="Cada variable tendrá su propia escala Y")
                    
                    med_df = st.session_state.db_manager.get_mediciones_chart_data(filters)
                    
                    if not med_df.empty:
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
                            highlight_points=st.session_state.measured_points if measure_mode else None
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
                
                # --- 8. i-STAT Custom View (Peces) ---
                try:
                    _istat_df = st.session_state.db_manager.con.execute("SELECT * FROM mediciones_data WHERE sheet_name = 'i-STAT'").df()
                    
                    if not _istat_df.empty:
                        st.markdown("---")
                        st.markdown("### 🐟 Explorador i-STAT (Muestreo de Peces)")
                        
                        _istat_df.columns = [str(c).strip() for c in _istat_df.columns]
                        
                        # Load Alertas por Estado for tooltips and coloring
                        try:
                            alertas_df = st.session_state.db_manager.con.execute("SELECT * FROM mediciones_data WHERE sheet_name = 'Alertas por Estado'").df()
                            alertas_dict = {}
                            for _, r in alertas_df.iterrows():
                                if pd.notna(r.get('Muestreo')) and pd.notna(r.get('Parámetro')) and pd.notna(r.get('Estado')):
                                    m = str(r['Muestreo']).strip().lower()
                                    p = str(r['Parámetro']).strip().lower()
                                    e = str(r['Estado']).strip().lower()
                                    alertas_dict[(m, p, e)] = (str(r.get('Nivel de Riesgo', '')).strip(), str(r.get('Explicación', '')).strip())
                        except Exception as e:
                            alertas_dict = {}
                            st.warning("⚠️ No se pudo cargar correctamente 'Alertas por Estado'.")
                        
                        # Use Departamento for first bubble selector
                        dept_col_istat = next((c for c in _istat_df.columns if c.lower() == 'departamento'), next((c for c in _istat_df.columns if 'departamento' in c.lower() or 'dept' in c.lower()), None))
                        
                        if dept_col_istat:
                            depts = sorted([d for d in _istat_df[dept_col_istat].dropna().unique() if str(d).strip()])
                            if depts:
                                st.markdown("**1. Selecciona un Lugar de Muestra (Departamento)**")
                                selected_dept = st.pills("Departamento:", depts, key="istat_dept", default=depts[0] if depts else None, label_visibility="collapsed")
                                
                                if selected_dept:
                                    dept_df = _istat_df[_istat_df[dept_col_istat] == selected_dept]
                                    
                                    m_col = next((c for c in dept_df.columns if c.lower() == 'muestreo' or 'muestreo' in c.lower() and 'fecha' not in c.lower()), None)
                                    f_col = next((c for c in dept_df.columns if 'fecha' in c.lower()), None)
                                    b_col = next((c for c in dept_df.columns if 'batch' in c.lower() or 'lote' in c.lower()), None)
                                    
                                    cols_to_group = [c for c in [m_col, f_col, b_col] if c]
                                    sort_cols = [c for c in [f_col, m_col] if c]
                                    
                                    # Sort by date descending
                                    summary_df = dept_df[cols_to_group].drop_duplicates().sort_values(by=sort_cols, ascending=False)
                                    
                                    st.markdown(f"**2. Selecciona un Evento de Muestreo ({len(summary_df)} encontrados)**")
                                    
                                    # Formatting dropdown options
                                    def format_event(row):
                                        parts = []
                                        if m_col and pd.notna(row.get(m_col)): parts.append(str(row[m_col]))
                                        if f_col and pd.notna(row.get(f_col)): 
                                            # Formatting date strictly if it's a timestamp
                                            d_val = row[f_col]
                                            if isinstance(d_val, pd.Timestamp):
                                                parts.append(d_val.strftime('%Y-%m-%d'))
                                            else:
                                                parts.append(str(d_val))
                                        if b_col and pd.notna(row.get(b_col)): parts.append(str(row[b_col]))
                                        return " | ".join(parts) if parts else "Desconocido"
                                        
                                    # Create selection list
                                    options = [format_event(row) for _, row in summary_df.iterrows()]
                                    selected_event_str = st.selectbox("Evento:", options=options, label_visibility="collapsed", key="istat_selection")
                                    
                                    if selected_event_str:
                                        # Find the original row that matches this string
                                        selected_idx = options.index(selected_event_str)
                                        selected_row = summary_df.iloc[selected_idx]
                                        
                                        # Filter original istat_df for this sampling
                                        mask = pd.Series(True, index=dept_df.index)
                                        if m_col: mask &= (dept_df[m_col] == selected_row[m_col])
                                        if f_col: mask &= (dept_df[f_col] == selected_row[f_col])
                                        if b_col: mask &= (dept_df[b_col] == selected_row[b_col])
                                        
                                        fish_df = dept_df[mask]
                                        
                                        st.markdown("---")
                                        st.markdown(f"**3. Peces evaluados en el muestreo seleccionado ({len(fish_df)}):**")
                                        
                                        # Define columns correctly based on user explicitly
                                        res_keywords = ['Glucosa', 'Hematocrito', 'Diox Carb', 'pH ac-bas', 'Bicarbonato', 'Na', 'K', 'Cl']
                                        
                                        # Render cards in a grid 3 per row
                                        cols_per_row = 3
                                        
                                        for r_idx in range(0, len(fish_df), cols_per_row):
                                            row_fishes = fish_df.iloc[r_idx: r_idx + cols_per_row]
                                            f_cols = st.columns(cols_per_row)
                                            
                                            for c_idx, (_, fish) in enumerate(row_fishes.iterrows()):
                                                with f_cols[c_idx]:
                                                    fish_num = r_idx + c_idx + 1
                                                    
                                                    # Which Muestreo string do we use for the lookup?
                                                    m_val = str(fish[m_col]).strip() if m_col and pd.notna(fish[m_col]) else ""
                                                    
                                                    # Generate Blood Results blocks for inside the fish
                                                    res_html = ""
                                                    for rk in res_keywords:
                                                        # EXACT matching only (ignoring case and outer whitespace)
                                                        match_c = next((c for c in fish.index if rk.strip().lower() == str(c).strip().lower()), None)
                                                        
                                                        val = "-"
                                                        color = "#1A1D24" # Normal text color
                                                        tooltip = ""
                                                        
                                                        if match_c and pd.notna(fish[match_c]) and str(fish[match_c]).strip():
                                                            raw_val = str(fish[match_c]).strip()
                                                            try:
                                                                v = float(raw_val)
                                                                val = f"{v:.1f}"
                                                            except:
                                                                val = raw_val
                                                                
                                                            # Check dictionary for Alerta tooltip/color
                                                            key = (m_val.lower(), rk.strip().lower(), raw_val.lower())
                                                            if key in alertas_dict:
                                                                riesgo, expl = alertas_dict[key]
                                                                if 'Crítico' in riesgo or '🔴' in riesgo:
                                                                    color = '#D9381E' # Red
                                                                elif 'Alerta' in riesgo or '🟠' in riesgo:
                                                                    color = '#FF9500' # Orange
                                                                else:
                                                                    color = '#1A1D24' # Normal Black
                                                                
                                                                tooltip = expl.replace('"', '&quot;')
                                                        
                                                        display_name = rk[:6].capitalize() if len(rk) > 6 else rk.capitalize()
                                                        res_html += f"<div title=\"{tooltip}\" style='font-family:-apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif; font-size:4.5px; color:{color}; line-height:1.2; font-weight:500; text-align:left; padding:0 2px; pointer-events:auto; cursor:help;'>{display_name}: <span style='font-weight:800; font-size:5px;'>{val}</span></div>"
                                                    
                                                    # Generate Bio Info below the fish
                                                    peso_col = next((c for c in fish.index if 'peso' in str(c).lower() and '(grs)' in str(c).lower()), None)
                                                    if not peso_col: peso_col = next((c for c in fish.index if 'peso' in str(c).lower()), None)
                                                    long_col = next((c for c in fish.index if 'longitud' in str(c).lower() and '(cm)' in str(c).lower()), None)
                                                    if not long_col: long_col = next((c for c in fish.index if 'longitud' in str(c).lower()), None)
                                                    
                                                    peso_val = fish[peso_col] if peso_col and pd.notna(fish[peso_col]) else "---"
                                                    long_val = fish[long_col] if long_col and pd.notna(fish[long_col]) else "---"
                                                    if isinstance(peso_val, (float, int)): peso_val = f"{peso_val:.1f}g"
                                                    if isinstance(long_val, (float, int)): long_val = f"{long_val:.1f}cm"

                                                    # Fish SVG path (Stylized Salmon/Trout shape)
                                                    import textwrap
                                                    
                                                    fish_svg = textwrap.dedent(f"""
                                                    <div style="position:relative; width:100%; flex-grow:1; min-height:80px; display:flex; align-items:center; justify-content:center; margin:0; padding:0;">
                                                        <svg viewBox="0 0 200 110" preserveAspectRatio="xMidYMid meet" style="width:100%; height:100%; display:block; filter:drop-shadow(0px 3px 4px rgba(0,0,0,0.4)); pointer-events:none;">
                                                            <defs>
                                                                <linearGradient id="fishGrad_{r_idx}_{c_idx}" x1="0%" y1="0%" x2="0%" y2="100%">
                                                                    <stop offset="0%" stop-color="#7B9EAD" />
                                                                    <stop offset="45%" stop-color="#D9E6ED" />
                                                                    <stop offset="100%" stop-color="#FDFBF7" />
                                                                </linearGradient>
                                                            </defs>
                                                            
                                                            <!-- Tail fin -->
                                                            <path d="M 30,55 L 5,25 L 15,55 L 5,85 Z" fill="#7B9EAD" stroke="#2B3A41" stroke-width="1" stroke-linejoin="round"/>
                                                            
                                                            <!-- Dorsal fin -->
                                                            <path d="M 100,25 C 110,3 135,10 130,27 Z" fill="#7B9EAD" stroke="#2B3A41" stroke-width="1"/>
                                                            
                                                            <!-- Pectoral fin -->
                                                            <path d="M 145,80 C 130,100 110,90 120,70 Z" fill="#7B9EAD" stroke="#2B3A41" stroke-width="1"/>
                                                            
                                                            <!-- Main Body (Harmonious Salmon Shape) -->
                                                            <path d="M 30,55 
                                                                     C 50,20 130,10 170,40 
                                                                     C 192,50 198,55 195,59 
                                                                     C 190,70 180,80 160,85 
                                                                     C 130,93 65,100 30,55 Z" 
                                                                  fill="url(#fishGrad_{r_idx}_{c_idx})" stroke="#2B3A41" stroke-width="1.5"/>
                                                            
                                                            <!-- Eye -->
                                                            <circle cx="170" cy="50" r="4" fill="#1A1D24"/>
                                                            <circle cx="171" cy="49" r="1.5" fill="#FAFAFA"/>
                                                            
                                                            <foreignObject x="55" y="30" width="105" height="50" style="pointer-events:none;">
                                                                <div xmlns="http://www.w3.org/1999/xhtml" style="width:100%; height:100%; display:grid; grid-template-columns:repeat(2, 1fr); gap:1.5px; align-items:center; justify-content:center; overflow:hidden; padding:0; pointer-events:none;">
                                                                    {res_html}
                                                                </div>
                                                            </foreignObject>
                                                        </svg>
                                                    </div>
                                                    """)

                                                    card_html = textwrap.dedent(f"""
                                                    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; overflow:hidden; margin:0; padding:0; height:100%; display:flex; flex-direction:column;">
                                                        <div style="text-align:center; font-size:12px; color:#4ECDC4; font-weight:700; flex-shrink:0;">Pez {fish_num}</div>
                                                        <div style="flex-grow:1; display:flex; align-items:center; justify-content:center; margin-bottom: 2px;">
                                                            {fish_svg}
                                                        </div>
                                                        
                                                        <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; color:#FAFAFA; font-size:11px; font-weight:600; text-align:center; flex-shrink:0;">
                                                            <div style="display:flex; align-items:center; justify-content:center; gap:6px;">
                                                                <span style="color:#A0AEC0; font-size:9px; text-transform:uppercase;">Longitud</span>
                                                                <div style="width:15px; height:1px; background:rgba(255,255,255,0.3);"></div>
                                                                <span>{long_val}</span>
                                                            </div>
                                                            <div style="margin-top:2px;">
                                                                <span style="color:#A0AEC0; font-size:9px; text-transform:uppercase;">Peso</span> {peso_val}
                                                            </div>
                                                        </div>
                                                    </div>
                                                    """)
                                                    # Use components.v1.html to absolutely force raw HTML rendering without Markdown interference
                                                    import streamlit.components.v1 as components
                                                    components.html(card_html, height=230, scrolling=False)
                                else:
                                    st.warning("⚠️ No se encontró la columna 'Departamento' para habilitar esta vista.")
                                    with st.expander("Ver columnas disponibles"):
                                        st.write(_istat_df.columns.tolist())
                except Exception as e:
                    st.error(f"Error cargando vista i-STAT: {str(e)}")

                st.markdown("---")
    
                # 3. Data Table (Collapsible)
                with st.expander("Ver Datos Detallados", expanded=False):
                    if filters.get('sum_units'):
                         st.info("ℹ️ Modo 'Sumar Unidades' activo: Los datos están agrupados por Lote y Fecha.")
                    st.dataframe(display_df, use_container_width=True)
    
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
