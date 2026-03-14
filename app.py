import streamlit as st
import pandas as pd
import json
import datetime as dt
import copy
import hashlib
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


def _resolve_col_name(columns, target: str):
    target_low = str(target).lower()
    for c in columns:
        if str(c).lower() == target_low:
            return c
    for c in columns:
        if target_low in str(c).lower():
            return c
    return None


def _build_quick_cards(filtered_df: pd.DataFrame, chart_vars: list, alias_map: dict, filters: dict, x_mode: str):
    if filtered_df is None or filtered_df.empty or not chart_vars:
        return []

    cols = list(filtered_df.columns)
    batch_col = _resolve_col_name(cols, 'batch') or _resolve_col_name(cols, 'lote')
    date_col = _resolve_col_name(cols, 'final fecha') or _resolve_col_name(cols, 'fecha') or _resolve_col_name(cols, 'date')

    x_col = None
    if filters.get('granularity') == 'Semana' and 'Semana' in filtered_df.columns:
        x_col = 'Semana'
    elif x_mode == 'Days':
        x_col = (
            _resolve_col_name(cols, 'final days since first input')
            or _resolve_col_name(cols, 'primer ingreso')
            or _resolve_col_name(cols, 'days')
        )
    if not x_col:
        x_col = date_col

    if batch_col:
        batch_values = [b for b in filtered_df[batch_col].dropna().astype(str).unique()]
        batch_values = sorted(batch_values)
    else:
        batch_values = ['Total']

    cards = []
    for batch_name in batch_values:
        if batch_col:
            batch_df = filtered_df[filtered_df[batch_col].astype(str) == str(batch_name)].copy()
        else:
            batch_df = filtered_df.copy()

        if batch_df.empty:
            continue

        for var in chart_vars:
            var_col = _resolve_col_name(batch_df.columns, var)
            if not var_col:
                continue

            values = pd.to_numeric(batch_df[var_col], errors='coerce')
            valid_mask = values.notna()
            if not valid_mask.any():
                continue

            vdf = batch_df.loc[valid_mask].copy()
            vvals = values.loc[valid_mask]

            v_min = float(vvals.min())
            v_max = float(vvals.max())
            v_avg = float(vvals.mean())

            last_val = None
            last_label = ""

            if date_col and date_col in vdf.columns:
                dts = pd.to_datetime(vdf[date_col], errors='coerce')
                if dts.notna().any():
                    dmax = dts.max()
                    last_vals = pd.to_numeric(vdf.loc[dts == dmax, var_col], errors='coerce').dropna()
                    if not last_vals.empty:
                        last_val = float(last_vals.mean())
                        last_label = dmax.strftime('%d-%m-%Y')

            if last_val is None and x_col and x_col in vdf.columns:
                x_num = pd.to_numeric(vdf[x_col], errors='coerce')
                if x_num.notna().any():
                    xmax = x_num.max()
                    last_vals = pd.to_numeric(vdf.loc[x_num == xmax, var_col], errors='coerce').dropna()
                    if not last_vals.empty:
                        last_val = float(last_vals.mean())
                        last_label = f"{x_col}: {xmax:g}"

            if last_val is None:
                try:
                    last_val = float(vvals.iloc[-1])
                except Exception:
                    last_val = v_avg

            card_id = f"{batch_name}|{var}"
            cards.append({
                'id': card_id,
                'batch': str(batch_name),
                'var': str(alias_map.get(var, var)),
                'raw_var': str(var),
                'last': float(last_val),
                'last_label': last_label,
                'min': v_min,
                'max': v_max,
                'avg': v_avg,
            })

    cards.sort(key=lambda c: (c['var'].lower(), c['batch'].lower()))
    return cards


def _card_btn_key(prefix: str, card_id: str):
    h = hashlib.md5(str(card_id).encode('utf-8')).hexdigest()[:10]
    return f"{prefix}_{h}"


def _render_quick_cards_main(cards):
    if not cards:
        return

    all_ids = [c['id'] for c in cards]
    if 'quick_cards_visible' not in st.session_state:
        st.session_state.quick_cards_visible = all_ids[:6]
    else:
        st.session_state.quick_cards_visible = [i for i in st.session_state.quick_cards_visible if i in all_ids]
        if not st.session_state.quick_cards_visible:
            st.session_state.quick_cards_visible = all_ids[:6]

    # enforce max 6 visible in main area (2 rows x 3)
    st.session_state.quick_cards_visible = st.session_state.quick_cards_visible[:6]

    card_map = {c['id']: c for c in cards}
    visible_cards = [card_map[i] for i in st.session_state.quick_cards_visible if i in card_map]

    if not visible_cards:
        return

    cols_per_row = 3
    for i in range(0, len(visible_cards), cols_per_row):
        row_cards = visible_cards[i:i + cols_per_row]
        cols = st.columns(cols_per_row)
        for idx, card in enumerate(row_cards):
            with cols[idx]:
                card_html = f"""
<div style="background:linear-gradient(160deg, rgba(255,255,255,0.07), rgba(0,0,0,0.30)); border:1px solid rgba(255,255,255,0.14); border-radius:14px; padding:12px 14px; box-shadow:0 8px 24px rgba(0,0,0,0.35);">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
    <div style="font-size:0.74rem; color:#D8DEE9; font-weight:700;">{card['batch']}</div>
    <div style="font-size:0.68rem; color:rgba(255,255,255,0.58);">{card['var']}</div>
  </div>
  <div style="margin-bottom:10px;">
    <div style="font-size:0.62rem; color:rgba(255,255,255,0.55);">Último valor {('(' + card['last_label'] + ')') if card['last_label'] else ''}</div>
    <div style="font-size:1.18rem; font-weight:700; color:#FAFAFA;">{card['last']:,.3f}</div>
  </div>
  <div style="display:flex; justify-content:space-between; gap:8px; font-size:0.72rem;">
    <div><span style="color:rgba(255,255,255,0.55);">Mín:</span> <b>{card['min']:,.3f}</b></div>
    <div><span style="color:rgba(255,255,255,0.55);">Prom:</span> <b>{card['avg']:,.3f}</b></div>
    <div><span style="color:rgba(255,255,255,0.55);">Máx:</span> <b>{card['max']:,.3f}</b></div>
  </div>
</div>
"""
                st.markdown(card_html, unsafe_allow_html=True)
                if st.button("✕", key=_card_btn_key('qc_rm', card['id']), help="Quitar de vista principal"):
                    st.session_state.quick_cards_visible = [
                        cid for cid in st.session_state.quick_cards_visible if cid != card['id']
                    ]
                    st.rerun()

    hidden_count = max(0, len(cards) - len(visible_cards))
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if st.button(
            f"🗂 Tarjetas ocultas ({hidden_count})",
            key="open_cards_view_btn_bottom",
            use_container_width=True,
            disabled=(hidden_count == 0),
        ):
            show_view_transition()
            st.session_state.current_view = "Cards"
            st.rerun()


def _render_quick_cards_screen():
    cards = st.session_state.get('quick_cards_all', [])
    if not cards:
        st.info("Aún no hay tarjetas generadas. Vuelve a la vista principal y genera un gráfico.")
        return

    all_ids = [c['id'] for c in cards]
    if 'quick_cards_visible' not in st.session_state:
        st.session_state.quick_cards_visible = all_ids[:6]
    visible_ids = st.session_state.quick_cards_visible[:6]

    hidden_cards = [c for c in cards if c['id'] not in visible_ids]

    st.markdown("### Tarjetas guardadas")
    st.caption("Aquí se muestran las tarjetas no visibles en la vista principal. Puedes añadir hasta 6 abajo del gráfico.")

    if not hidden_cards:
        st.success("No hay tarjetas ocultas. Todas las disponibles ya están visibles (máximo 6).")
        return

    for card in hidden_cards:
        c1, c2 = st.columns([8, 1])
        with c1:
            st.markdown(
                f"**{card['batch']} · {card['var']}** — Último: `{card['last']:.3f}` · Min: `{card['min']:.3f}` · Prom: `{card['avg']:.3f}` · Max: `{card['max']:.3f}`"
            )
        with c2:
            if st.button("＋", key=_card_btn_key('qc_add', card['id']), help="Añadir a vista principal"):
                vis = list(st.session_state.quick_cards_visible)
                if card['id'] not in vis:
                    if len(vis) >= 6:
                        st.warning("Ya hay 6 tarjetas visibles. Quita una (✕) antes de añadir otra.")
                    else:
                        vis.append(card['id'])
                        st.session_state.quick_cards_visible = vis
                        st.rerun()


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
    elif st.session_state.current_view == "Cards":
        col_header, col_spacer, col_btn_main = st.columns([2, 5, 1])
        with col_header:
            inject_logo(dashboard_mode=True)
        with col_btn_main:
            if st.button("⬅️", key="back_to_main_from_cards_btn", help="Volver a datos y gráficos", use_container_width=True):
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
                        ['Líneas', 'Líneas + Marcadores', 'Barras', 'Área', 'Dispersión', 'Torta', 'Treemap', 'Pareto'],
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
                        
                        # Quick cards per displayed variable + batch (max 6 visible below chart)
                        quick_cards = _build_quick_cards(
                            filtered_df=filtered_df,
                            chart_vars=actual_chart_vars,
                            alias_map=alias_map,
                            filters=filters,
                            x_mode=x_mode,
                        )
                        st.session_state.quick_cards_all = quick_cards
                        _render_quick_cards_main(quick_cards)
                        
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
    elif st.session_state.current_view == "Cards":
        _render_quick_cards_screen()

    # Empty state when NO data loaded
    if 'data_loaded' not in st.session_state or not st.session_state.data_loaded:
        st.info("Aún no hay datos cargados en la base. Usa 'Cargar / Actualizar datos' en el panel lateral.")

if __name__ == "__main__":
    main()
