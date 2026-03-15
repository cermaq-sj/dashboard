import streamlit as st
import pandas as pd
import json
import datetime as dt
import copy
import hashlib
import plotly.io as pio
from src.filters import render_filters
from src.data_processing import load_and_clean_data
from src.db_manager import DBManager
from src.visualizations import create_main_chart
from src.config_params import render_config_tab, get_range_filters, get_alias_map, get_kpi_config_thresholds, ensure_runtime_config
from src.styles import inject_styles, inject_logo, show_loading_screen, hide_loading_screen, show_view_transition
import traceback
import unicodedata
import base64

try:
    from streamlit_plotly_events import plotly_events as _plotly_events
    PLOTLY_EVENTS_AVAILABLE = True
except Exception:
    _plotly_events = None
    PLOTLY_EVENTS_AVAILABLE = False

try:
    from streamlit_elements import elements, dashboard, mui, html, sync
    ELEMENTS_AVAILABLE = True
except Exception:
    elements = None
    dashboard = None
    mui = None
    html = None
    sync = None
    ELEMENTS_AVAILABLE = False


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


def _load_dashboard_profiles():
    profiles = st.session_state.db_manager.list_dashboard_profiles()
    if not profiles:
        return [{'profile_key': 'maestro', 'profile_name': 'Maestro'}]
    return profiles


def _apply_fcr_mode_to_chart_vars(chart_vars: list, filtered_df: pd.DataFrame, fcr_view_mode: str):
    if fcr_view_mode != "Vista individual":
        return list(chart_vars)

    cols = list(filtered_df.columns)

    def _find_exact(*names):
        target = {str(n).strip().lower() for n in names}
        return next((c for c in cols if c.strip().lower() in target), None)

    def _find_contains(*parts):
        parts_low = [str(p).lower() for p in parts]
        for c in cols:
            cl = c.strip().lower()
            if all(p in cl for p in parts_low):
                return c
        return None

    out = list(chart_vars)
    swaps = [
        ("FCR Económico Acumulado", lambda: _find_exact('final fcr economico', 'final fcr económico')),
        ("FCR Biológico Acumulado", lambda: _find_exact('final fcr biologico', 'final fcr biológico')),
        ("GF3 Acumulado", lambda: _find_exact('final gf3')),
        ("SGR Acumulado", lambda: _find_exact('final sgr')),
        ("SFR Acumulado", lambda: _find_exact('final sfr')),
        ("% Mortalidad Acumulada", lambda: _find_exact('final mortalidad, porcentaje')),
        ("% Mortalidad diaria", lambda: _find_contains('mortalidad', 'porcentaje', 'per')),
        ("% Pérdida Acumulada", lambda: _find_contains('perdida', 'numero', 'periodo')),
        ("% Eliminación Acumulada", lambda: _find_contains('eliminados', 'numero', 'periodo')),
        ("Pérdida diaria %", lambda: _find_contains('perdida', 'numero', 'periodo')),
        ("Eliminación diaria %", lambda: _find_contains('eliminados', 'numero', 'periodo')),
        ("Peso promedio", lambda: _find_exact('final peso prom')),
    ]

    for source_name, resolver in swaps:
        if source_name in out:
            out = [v for v in out if v != source_name]
            resolved = resolver()
            if resolved and resolved not in out:
                out.append(resolved)

    return out


def _resolve_actual_chart_vars(chart_vars: list, chart_type: str, pie_view_mode: str):
    if chart_type != 'Torta':
        return list(chart_vars)

    actual_chart_vars = list(chart_vars)
    is_trio = (
        set(v.strip().lower() for v in actual_chart_vars)
        == {"% pérdida acumulada", "% eliminación acumulada", "% mortalidad acumulada"}
    )

    cause_names = [
        'Embrionaria', 'Deforme Embrionaria', 'Micosis', 'Daño Mecánico Otros',
        'Desadaptado', 'Deforme', 'Descompuesto', 'Aborto', 'Daño Mecánico',
        'Sin causa Aparente', 'Maduro', 'Muestras', 'Operculo Corto',
        'Rezagado', 'Nefrocalcinosis', 'Exofialosis', 'Daño Mecánico por Muestreo',
    ]

    if is_trio:
        causes_vars = [f"% Mortalidad {c} Acumulada" for c in cause_names]
        if pie_view_mode == "parents":
            actual_chart_vars = ["% Pérdida Acumulada"]
        elif pie_view_mode == "children":
            actual_chart_vars = ["% Eliminación Acumulada", "% Mortalidad Acumulada"]
        elif pie_view_mode == "causes":
            actual_chart_vars = ["% Eliminación Acumulada"] + causes_vars
    elif pie_view_mode == "causes":
        is_daily = any('diaria' in v.lower() for v in actual_chart_vars if 'mortalidad' in v.lower())
        cause_suffix = 'Diaria' if is_daily else 'Acumulada'
        causes_vars = [f"% Mortalidad {c} {cause_suffix}" for c in cause_names]
        actual_chart_vars = [v for v in actual_chart_vars if "mortalidad" not in v.lower() or "causa" in v.lower()]
        actual_chart_vars = actual_chart_vars + causes_vars

    return actual_chart_vars


def _build_snapshot_figure(snapshot_cfg: dict, data_version: str, kpi_thresholds: dict):
    cfg = copy.deepcopy(snapshot_cfg or {})
    filters = copy.deepcopy(cfg.get('filters') or {})
    selected_vars = list(cfg.get('selected_vars') or filters.get('variables') or [])

    if not filters:
        return None, "No hay filtros guardados para este gráfico."
    if not selected_vars:
        return None, "No hay variables guardadas para este gráfico."

    filtered_df = _cached_filtered_data(
        st.session_state.db_manager,
        data_version,
        _cache_key(filters),
    )
    if filtered_df is None or filtered_df.empty:
        return None, "No hay datos para la configuración guardada."

    variable_ranges_main = get_range_filters('fishtalk_data')
    alias_map = get_alias_map('fishtalk_data')

    chart_type = cfg.get('chart_type', 'Líneas')
    fcr_mode = cfg.get('fcr_view_mode', 'Vista general')
    pie_view_mode = cfg.get('pie_view_mode', 'parents')
    overlay_on = bool(cfg.get('overlay_on', False))
    unite_vars = bool(cfg.get('unite_vars', False))
    align_first = bool(cfg.get('align_first', False))

    chart_vars = _apply_fcr_mode_to_chart_vars(selected_vars, filtered_df, fcr_mode)
    actual_chart_vars = _resolve_actual_chart_vars(chart_vars, chart_type, pie_view_mode)
    if not actual_chart_vars:
        return None, "No hay variables válidas para construir el gráfico."

    selected_proj_vars = cfg.get('proyecciones_vars')
    if selected_proj_vars is None:
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

    fig = create_main_chart(
        filtered_df,
        actual_chart_vars,
        batch_comparison_mode='Overlay',
        x_axis_mode='Days' if overlay_on else 'Date',
        chart_type=chart_type,
        hover_mode='x unified',
        sum_units=filters.get('sum_units', False),
        avg_units=filters.get('avg_units', False),
        align_first=align_first,
        highlight_points=None,
        unite_variables=unite_vars,
        rename_map=alias_map,
        pie_view_mode=pie_view_mode,
        kpi_thresholds=kpi_thresholds if filters.get('active_kpis') else None,
        active_kpis=filters.get('active_kpis', []),
        proyecciones_df=proj_df_for_chart,
        variable_ranges=variable_ranges_main,
        uirevision_key=None,
    )
    return fig, None


DASH_COLS = 12
DASH_ROW_HEIGHT = 46
DASH_GAP = 8


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_tile_size(value: str) -> str:
    v = str(value or '').strip().lower()
    return v if v in {'small', 'medium', 'large', 'full'} else 'large'


def _size_to_default_wh(size_name: str, tile_type: str):
    s = _normalize_tile_size(size_name)
    if s == 'small':
        return 3, 4 if tile_type == 'quick_card' else 6
    if s == 'medium':
        return 6, 5 if tile_type == 'quick_card' else 7
    if s == 'full':
        return 12, 6 if tile_type == 'quick_card' else 8
    return 9, 5 if tile_type == 'quick_card' else 7


def _layout_signature(layout_map: dict) -> str:
    normalized = {}
    for k, v in (layout_map or {}).items():
        normalized[str(k)] = {
            'x': _safe_int((v or {}).get('x'), 0),
            'y': _safe_int((v or {}).get('y'), 0),
            'w': _safe_int((v or {}).get('w'), 6),
            'h': _safe_int((v or {}).get('h'), 6),
        }
    return _cache_key(normalized)


def _ensure_grid_layout(profile_key: str, items: list) -> list:
    prepared = []
    for idx, item in enumerate(items):
        cpy = copy.deepcopy(item)
        cfg = cpy.get('config') or {}
        cpy['config'] = cfg
        layout = copy.deepcopy(cfg.get('layout') or {})
        cfg['layout'] = layout
        layout.setdefault('order', idx)
        cfg.setdefault('tile_type', 'chart')
        prepared.append(cpy)

    prepared.sort(key=lambda it: _safe_int(((it.get('config') or {}).get('layout') or {}).get('order'), 999999))

    changed = []
    cursor_x = 0
    cursor_y = 0
    row_h = 0

    for idx, item in enumerate(prepared):
        cfg = item.get('config') or {}
        tile_type = str(cfg.get('tile_type') or 'chart')
        layout = cfg.get('layout') or {}
        width_default, height_default = _size_to_default_wh(layout.get('size', 'large'), tile_type)

        has_xywh = all(k in layout for k in ['x', 'y', 'w', 'h'])
        if has_xywh:
            x = max(0, min(DASH_COLS - 1, _safe_int(layout.get('x'), 0)))
            y = max(0, _safe_int(layout.get('y'), 0))
            w = max(2, min(DASH_COLS, _safe_int(layout.get('w'), width_default)))
            h = max(3, _safe_int(layout.get('h'), height_default))
        else:
            w = width_default
            h = height_default
            if cursor_x + w > DASH_COLS:
                cursor_x = 0
                cursor_y += max(1, row_h)
                row_h = 0
            x = cursor_x
            y = cursor_y
            cursor_x += w
            row_h = max(row_h, h)

        new_layout = {
            'x': x,
            'y': y,
            'w': w,
            'h': h,
            'size': _normalize_tile_size(layout.get('size', 'large')),
            'order': idx,
        }

        if _layout_signature({'a': layout}) != _layout_signature({'a': new_layout}):
            changed.append((item.get('chart_id', ''), cfg, new_layout))

        cfg['layout'] = new_layout

    for chart_id, cfg, new_layout in changed:
        if not chart_id:
            continue
        cfg_to_save = copy.deepcopy(cfg)
        cfg_to_save['layout'] = new_layout
        st.session_state.db_manager.update_dashboard_chart_config(chart_id, _to_jsonable(cfg_to_save))

    return prepared


def _next_profile_order(profile_key: str) -> int:
    items = st.session_state.db_manager.list_dashboard_profile_charts(profile_key)
    if not items:
        return 0
    max_order = -1
    for item in items:
        cfg = item.get('config') or {}
        layout = cfg.get('layout') or {}
        max_order = max(max_order, _safe_int(layout.get('order'), 0))
    return int(max_order + 1)


def _extract_layout_payload(event_payload):
    if event_payload is None:
        return []
    if isinstance(event_payload, list):
        if event_payload and isinstance(event_payload[0], dict) and 'i' in event_payload[0]:
            return event_payload
        if event_payload and isinstance(event_payload[0], list):
            first = event_payload[0]
            if first and isinstance(first[0], dict) and 'i' in first[0]:
                return first
    if isinstance(event_payload, dict):
        if 'lg' in event_payload and isinstance(event_payload['lg'], list):
            return event_payload['lg']
    return []


def _persist_profile_layout(profile_key: str, items: list, layout_payload: list):
    if not layout_payload:
        return False

    by_id = {}
    for raw in layout_payload:
        item_id = str(raw.get('i') or '')
        if not item_id:
            continue
        by_id[item_id] = {
            'x': max(0, min(DASH_COLS - 1, _safe_int(raw.get('x'), 0))),
            'y': max(0, _safe_int(raw.get('y'), 0)),
            'w': max(2, min(DASH_COLS, _safe_int(raw.get('w'), 6))),
            'h': max(3, _safe_int(raw.get('h'), 6)),
        }

    if not by_id:
        return False

    cache_key = f"dash_layout_sig_{profile_key}"
    new_sig = _layout_signature(by_id)
    if st.session_state.get(cache_key) == new_sig:
        return False

    changed = False
    for order_idx, item in enumerate(sorted(layout_payload, key=lambda x: (_safe_int(x.get('y'), 0), _safe_int(x.get('x'), 0)))):
        chart_id = str(item.get('i') or '')
        if not chart_id:
            continue
        existing = next((it for it in items if str(it.get('chart_id')) == chart_id), None)
        if not existing:
            continue
        cfg = copy.deepcopy(existing.get('config') or {})
        layout = copy.deepcopy(cfg.get('layout') or {})
        target = by_id.get(chart_id)
        if not target:
            continue
        merged = {
            'x': target['x'],
            'y': target['y'],
            'w': target['w'],
            'h': target['h'],
            'size': _normalize_tile_size(layout.get('size', 'large')),
            'order': order_idx,
        }
        if _layout_signature({'a': layout}) == _layout_signature({'a': merged}):
            continue
        cfg['layout'] = merged
        ok = st.session_state.db_manager.update_dashboard_chart_config(chart_id, _to_jsonable(cfg))
        changed = changed or bool(ok)

    st.session_state[cache_key] = new_sig
    return changed


@st.cache_data(ttl=300, max_entries=128, show_spinner=False)
def _build_plotly_iframe_srcdoc(figure_json: str) -> str:
    fig = pio.from_json(str(figure_json or '{}'))
    fig_html = fig.to_html(include_plotlyjs=True, full_html=False, config={'displaylogo': False, 'responsive': True})
    return (
        "<html><head><meta charset='utf-8'><style>html,body{margin:0;padding:0;height:100%;overflow:hidden;background:transparent;}"
        "#wrap{height:100%;width:100%;}</style></head><body><div id='wrap'>"
        + fig_html
        + "</div></body></html>"
    )


def _html_to_data_uri(doc: str) -> str:
    encoded = base64.b64encode(str(doc or '').encode('utf-8')).decode('ascii')
    return f"data:text/html;base64,{encoded}"


def _quick_card_html(card: dict) -> str:
    if not card:
        return "<div style='padding:8px;color:#ddd;'>Tarjeta sin datos</div>"
    label = str(card.get('var', 'Variable'))
    batch = str(card.get('batch', 'Total'))
    last = float(card.get('last', 0.0) or 0.0)
    vmin = float(card.get('min', 0.0) or 0.0)
    vmax = float(card.get('max', 0.0) or 0.0)
    vavg = float(card.get('avg', 0.0) or 0.0)
    last_label = str(card.get('last_label') or '')
    extra = f" ({last_label})" if last_label else ""
    return f"""
<div style="height:100%;background:linear-gradient(160deg, rgba(255,255,255,0.06), rgba(0,0,0,0.28)); border:1px solid rgba(255,255,255,0.10); border-radius:8px; padding:8px 10px; box-sizing:border-box; color:#f2f2f2; font-family:system-ui, sans-serif;">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
    <div style="font-size:0.70rem; color:#D8DEE9; font-weight:700;">{batch}</div>
    <div style="font-size:0.66rem; color:rgba(255,255,255,0.62);">{label}</div>
  </div>
  <div style="margin-bottom:6px;">
    <div style="font-size:0.60rem; color:rgba(255,255,255,0.55);">Ultimo valor{extra}</div>
    <div style="font-size:1.05rem; font-weight:700; color:#FAFAFA;">{last:,.3f}</div>
  </div>
  <div style="display:flex; justify-content:space-between; gap:6px; font-size:0.68rem;">
    <div><span style="color:rgba(255,255,255,0.55);">Min:</span> <b>{vmin:,.3f}</b></div>
    <div><span style="color:rgba(255,255,255,0.55);">Prom:</span> <b>{vavg:,.3f}</b></div>
    <div><span style="color:rgba(255,255,255,0.55);">Max:</span> <b>{vmax:,.3f}</b></div>
  </div>
</div>
"""


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

    profiles = _load_dashboard_profiles()
    current_profile = st.session_state.get('current_profile', 'maestro')
    available_keys = [p['profile_key'] for p in profiles]
    if current_profile not in available_keys and available_keys:
        current_profile = available_keys[0]
        st.session_state.current_profile = current_profile

    for item in profiles:
        p_key = item.get('profile_key', 'maestro')
        p_name = item.get('profile_name', p_key)
        btn_type = "primary" if current_profile == p_key else "secondary"
        if st.button(p_name, key=f"btn_profile_{p_key}", type=btn_type, use_container_width=True):
            st.session_state.current_profile = p_key
            st.rerun()


@st.dialog("➕ Guardar gráfico en Dashboard", width="small")
def show_save_chart_dialog():
    pending = st.session_state.get('pending_dashboard_snapshot')
    if not pending:
        st.info("No hay gráfico listo para guardar.")
        return

    profiles = _load_dashboard_profiles()
    profile_keys = [p['profile_key'] for p in profiles]
    profile_name_map = {p['profile_key']: p.get('profile_name', p['profile_key']) for p in profiles}

    current_profile = st.session_state.get('current_profile', 'maestro')
    if current_profile not in profile_keys and profile_keys:
        current_profile = profile_keys[0]

    selected_profile = st.selectbox(
        "Perfil",
        options=profile_keys,
        index=profile_keys.index(current_profile) if current_profile in profile_keys else 0,
        format_func=lambda k: profile_name_map.get(k, k),
        key="save_dash_profile_select",
    )
    new_profile_name = st.text_input("O crear perfil nuevo", placeholder="Ej: Producción marzo", key="save_dash_new_profile")
    chart_title = st.text_input(
        "Nombre del gráfico",
        value=str(pending.get('chart_title') or 'Gráfico guardado'),
        key="save_dash_chart_title",
    )

    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("Guardar", type="primary", use_container_width=True, key="save_dash_chart_confirm"):
            target_profile = selected_profile
            if new_profile_name.strip():
                created = st.session_state.db_manager.create_dashboard_profile(new_profile_name.strip())
                if not created:
                    st.error("No se pudo crear el perfil nuevo.")
                    return
                target_profile = created['profile_key']

            next_order = _next_profile_order(target_profile)

            chart_id = st.session_state.db_manager.save_dashboard_chart_snapshot(
                profile_key=target_profile,
                chart_title=chart_title.strip() or "Gráfico guardado",
                figure_json=str(pending.get('figure_json') or ''),
                config_payload=_to_jsonable({
                    **(pending.get('config') or {}),
                    'tile_type': (pending.get('config') or {}).get('tile_type', 'chart'),
                    'layout': {
                        'x': 0,
                        'y': next_order * 8,
                        'w': 9,
                        'h': 7,
                        'size': _normalize_tile_size(((pending.get('config') or {}).get('layout') or {}).get('size', 'large')),
                        'order': next_order,
                    },
                }),
            )
            if chart_id:
                st.session_state.current_profile = target_profile
                st.session_state.pending_dashboard_snapshot = None
                st.success("Gráfico guardado en Dashboard.")
                st.rerun()
            else:
                st.error("No se pudo guardar el gráfico en la base de datos.")
    with col_cancel:
        if st.button("Cancelar", use_container_width=True, key="save_dash_chart_cancel"):
            st.session_state.pending_dashboard_snapshot = None
            st.rerun()


@st.dialog("⋯ Configurar gráfico guardado", width="small")
def show_dashboard_chart_settings_dialog(chart_id: str, data_version: str):
    chart = st.session_state.db_manager.get_dashboard_chart(chart_id)
    if not chart:
        st.error("No se encontró el gráfico seleccionado.")
        return

    cfg = copy.deepcopy(chart.get('config') or {})
    layout = copy.deepcopy(cfg.get('layout') or {})
    filters = copy.deepcopy(cfg.get('filters') or {})
    tile_type = str(cfg.get('tile_type') or 'chart')

    if tile_type == 'quick_card':
        st.caption("Tarjeta rápida")
        col_ok, col_cancel = st.columns(2)
        with col_ok:
            if st.button("OK", type="primary", use_container_width=True, key=f"dash_cfg_ok_qc_{chart_id}"):
                new_cfg = copy.deepcopy(cfg)
                new_cfg['tile_type'] = 'quick_card'
                new_cfg['layout'] = {
                    'x': _safe_int(layout.get('x'), 0),
                    'y': _safe_int(layout.get('y'), 0),
                    'w': _safe_int(layout.get('w'), 3),
                    'h': _safe_int(layout.get('h'), 4),
                    'size': _normalize_tile_size(layout.get('size', 'small')),
                    'order': _safe_int(layout.get('order'), 0),
                }
                ok = st.session_state.db_manager.update_dashboard_chart_config(chart_id, _to_jsonable(new_cfg))
                if ok:
                    st.success("Tarjeta actualizada.")
                    st.rerun()
                else:
                    st.error("No se pudo actualizar la tarjeta.")
        with col_cancel:
            if st.button("Cancelar", use_container_width=True, key=f"dash_cfg_cancel_qc_{chart_id}"):
                st.rerun()
        return

    st.caption(chart.get('chart_title') or "Gráfico guardado")

    available_batches = [str(v) for v in st.session_state.db_manager.get_unique_values("Lote")]
    saved_batches = [str(v) for v in filters.get('batches', [])]
    default_batches = [b for b in saved_batches if b in available_batches]

    batches = st.multiselect(
        "Batch",
        options=available_batches,
        default=default_batches,
        key=f"dash_cfg_batches_{chart_id}",
    )

    min_days, max_days = st.session_state.db_manager.get_min_max("Days")
    days_range = filters.get('days_range', [])
    has_days = min_days is not None and max_days is not None
    if has_days:
        min_d = int(min_days)
        max_d = int(max_days)
        default_days = (min_d, max_d)
        if isinstance(days_range, (list, tuple)) and len(days_range) == 2:
            try:
                d0 = int(days_range[0])
                d1 = int(days_range[1])
                d0 = max(min_d, min(max_d, d0))
                d1 = max(min_d, min(max_d, d1))
                if d0 > d1:
                    d0, d1 = d1, d0
                default_days = (d0, d1)
            except Exception:
                pass

        days_selected = st.slider(
            "Rango días de cultivo",
            min_value=min_d,
            max_value=max_d,
            value=default_days,
            key=f"dash_cfg_days_{chart_id}",
        )
    else:
        days_selected = days_range

    granularity_saved = str(filters.get('granularity', 'Día'))
    granularity = st.radio(
        "Agrupación temporal",
        options=["Día", "Semana"],
        index=1 if granularity_saved == "Semana" else 0,
        horizontal=True,
        key=f"dash_cfg_gran_{chart_id}",
    )

    overlay_on = st.checkbox(
        "Superponer",
        value=bool(cfg.get('overlay_on', False)),
        key=f"dash_cfg_overlay_{chart_id}",
    )
    unite_vars = st.checkbox(
        "Unir variables",
        value=bool(cfg.get('unite_vars', False)),
        key=f"dash_cfg_unite_{chart_id}",
    )
    align_first = st.checkbox(
        "Desde 1er reg.",
        value=bool(cfg.get('align_first', False)),
        key=f"dash_cfg_align_{chart_id}",
    )

    kpi_thresholds = get_kpi_config_thresholds() or _cached_kpi_thresholds(st.session_state.db_manager, data_version)

    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("OK", type="primary", use_container_width=True, key=f"dash_cfg_ok_{chart_id}"):
            new_cfg = copy.deepcopy(cfg)
            new_filters = copy.deepcopy(filters)
            new_filters['batches'] = batches
            new_filters['granularity'] = granularity
            if has_days and isinstance(days_selected, (list, tuple)) and len(days_selected) == 2:
                new_filters['days_range'] = [int(days_selected[0]), int(days_selected[1])]

            new_cfg['filters'] = new_filters
            new_cfg['tile_type'] = 'chart'
            new_cfg['overlay_on'] = bool(overlay_on)
            new_cfg['unite_vars'] = bool(unite_vars)
            new_cfg['align_first'] = bool(align_first)
            new_cfg['layout'] = {
                'x': _safe_int(layout.get('x'), 0),
                'y': _safe_int(layout.get('y'), 0),
                'w': _safe_int(layout.get('w'), 9),
                'h': _safe_int(layout.get('h'), 7),
                'size': _normalize_tile_size(layout.get('size', 'large')),
                'order': _safe_int(layout.get('order'), 0),
            }

            fig, err = _build_snapshot_figure(new_cfg, data_version, kpi_thresholds)
            if err:
                st.error(err)
                return

            ok = st.session_state.db_manager.update_dashboard_chart_snapshot(
                chart_id=chart_id,
                profile_key=chart.get('profile_key', st.session_state.get('current_profile', 'maestro')),
                chart_title=chart.get('chart_title') or 'Gráfico guardado',
                figure_json=fig.to_json(),
                config_payload=_to_jsonable(new_cfg),
            )
            if ok:
                st.success("Gráfico actualizado.")
                st.rerun()
            else:
                st.error("No se pudo actualizar el gráfico.")
    with col_cancel:
        if st.button("Cancelar", use_container_width=True, key=f"dash_cfg_cancel_{chart_id}"):
            st.rerun()


@st.dialog("➕ Agregar tarjetas rápidas al Dashboard", width="small")
def show_save_quick_cards_dialog():
    cards = st.session_state.get('pending_dashboard_quick_cards') or []
    if not cards:
        st.info("No hay tarjetas rápidas disponibles para agregar.")
        return

    profiles = _load_dashboard_profiles()
    profile_keys = [p['profile_key'] for p in profiles]
    profile_name_map = {p['profile_key']: p.get('profile_name', p['profile_key']) for p in profiles}

    current_profile = st.session_state.get('current_profile', 'maestro')
    if current_profile not in profile_keys and profile_keys:
        current_profile = profile_keys[0]

    selected_profile = st.selectbox(
        "Perfil",
        options=profile_keys,
        index=profile_keys.index(current_profile) if current_profile in profile_keys else 0,
        format_func=lambda k: profile_name_map.get(k, k),
        key="save_dash_qc_profile_select",
    )
    new_profile_name = st.text_input("O crear perfil nuevo", placeholder="Ej: Operación noche", key="save_dash_qc_new_profile")

    options = [c.get('id') for c in cards if c.get('id')]
    label_map = {
        c.get('id'): f"{c.get('batch', 'Total')} · {c.get('var', 'Variable')}"
        for c in cards if c.get('id')
    }
    default_selected = options[: min(6, len(options))]
    selected_cards = st.multiselect(
        "Tarjetas a agregar",
        options=options,
        default=default_selected,
        format_func=lambda cid: label_map.get(cid, cid),
        key="save_dash_qc_selected",
    )

    size_options = ['small', 'medium', 'large', 'full']
    size_labels = {
        'small': 'Pequeño (1/4)',
        'medium': 'Mediano (1/2)',
        'large': 'Grande (3/4)',
        'full': 'Pantalla (1/1)',
    }
    qc_size = st.selectbox(
        "Tamaño inicial",
        options=size_options,
        index=0,
        format_func=lambda s: size_labels.get(s, s),
        key="save_dash_qc_size",
    )

    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("Guardar", type="primary", use_container_width=True, key="save_dash_qc_confirm"):
            if not selected_cards:
                st.warning("Selecciona al menos una tarjeta.")
                return

            target_profile = selected_profile
            if new_profile_name.strip():
                created = st.session_state.db_manager.create_dashboard_profile(new_profile_name.strip())
                if not created:
                    st.error("No se pudo crear el perfil nuevo.")
                    return
                target_profile = created['profile_key']

            card_map = {c.get('id'): c for c in cards if c.get('id')}
            base_order = _next_profile_order(target_profile)
            saved = 0
            for idx, card_id in enumerate(selected_cards):
                card = card_map.get(card_id)
                if not card:
                    continue
                title = f"{card.get('batch', 'Total')} · {card.get('var', 'Variable')}"
                payload = {
                    'tile_type': 'quick_card',
                    'quick_card': _to_jsonable(card),
                    'layout': {
                        'x': 0,
                        'y': (base_order + idx) * 5,
                        'w': 3,
                        'h': 4,
                        'size': _normalize_tile_size(qc_size),
                        'order': base_order + idx,
                    },
                }
                chart_id = st.session_state.db_manager.save_dashboard_chart_snapshot(
                    profile_key=target_profile,
                    chart_title=title,
                    figure_json="",
                    config_payload=payload,
                )
                if chart_id:
                    saved += 1

            st.session_state.current_profile = target_profile
            st.session_state.pending_dashboard_quick_cards = None
            if saved > 0:
                st.success(f"Se agregaron {saved} tarjetas rápidas al Dashboard.")
                st.rerun()
            else:
                st.error("No se pudo guardar ninguna tarjeta rápida.")
    with col_cancel:
        if st.button("Cancelar", use_container_width=True, key="save_dash_qc_cancel"):
            st.session_state.pending_dashboard_quick_cards = None
            st.rerun()


def _render_quick_dashboard_card(card: dict):
    if not card:
        st.info("Tarjeta rápida sin datos.")
        return
    label = str(card.get('var', 'Variable'))
    batch = str(card.get('batch', 'Total'))
    last = float(card.get('last', 0.0) or 0.0)
    vmin = float(card.get('min', 0.0) or 0.0)
    vmax = float(card.get('max', 0.0) or 0.0)
    vavg = float(card.get('avg', 0.0) or 0.0)
    last_label = str(card.get('last_label') or '')

    st.markdown(
        f"""
<div style="background:linear-gradient(160deg, rgba(255,255,255,0.06), rgba(0,0,0,0.28)); border:1px solid rgba(255,255,255,0.10); border-radius:10px; padding:10px 12px;">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
    <div style="font-size:0.72rem; color:#D8DEE9; font-weight:700;">{batch}</div>
    <div style="font-size:0.68rem; color:rgba(255,255,255,0.62);">{label}</div>
  </div>
  <div style="margin-bottom:8px;">
    <div style="font-size:0.62rem; color:rgba(255,255,255,0.55);">Último valor {('(' + last_label + ')') if last_label else ''}</div>
    <div style="font-size:1.2rem; font-weight:700; color:#FAFAFA;">{last:,.3f}</div>
  </div>
  <div style="display:flex; justify-content:space-between; gap:8px; font-size:0.70rem;">
    <div><span style="color:rgba(255,255,255,0.55);">Mín:</span> <b>{vmin:,.3f}</b></div>
    <div><span style="color:rgba(255,255,255,0.55);">Prom:</span> <b>{vavg:,.3f}</b></div>
    <div><span style="color:rgba(255,255,255,0.55);">Máx:</span> <b>{vmax:,.3f}</b></div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_dashboard_content(data_version: str):
    profiles = _load_dashboard_profiles()
    name_map = {p['profile_key']: p.get('profile_name', p['profile_key']) for p in profiles}
    current_profile = st.session_state.get('current_profile', 'maestro')

    if current_profile not in name_map:
        current_profile = profiles[0]['profile_key'] if profiles else 'maestro'
        st.session_state.current_profile = current_profile

    st.markdown(
        """
<style>
div.block-container {max-width: 100% !important; padding-left: 0.65rem !important; padding-right: 0.65rem !important;}
div[data-testid="stVerticalBlockBorderWrapper"] {border-width: 1px !important; border-color: rgba(160,174,192,0.22) !important; border-radius: 9px !important;}
</style>
        """,
        unsafe_allow_html=True,
    )

    charts = st.session_state.db_manager.list_dashboard_profile_charts(current_profile)
    if not charts:
        st.info("Este perfil no tiene tarjetas guardadas aún. Ve a la vista principal y guarda gráficos o tarjetas rápidas.")
        return

    items = _ensure_grid_layout(current_profile, charts)

    if not ELEMENTS_AVAILABLE:
        st.warning("Instala `streamlit-elements` para mover tarjetas desde ⋯ y redimensionarlas desde la esquina inferior derecha.")
        for item in items:
            chart_id = item.get('chart_id', '')
            cfg = item.get('config') or {}
            layout = cfg.get('layout') or {}
            title = item.get('chart_title') or 'Tarjeta'
            updated_at = str(item.get('updated_at') or '')
            tooltip = title if not updated_at else f"{title} | Actualizado: {updated_at[:19]}"
            with st.container(border=True):
                c1, c2 = st.columns([1, 20])
                with c1:
                    if st.button("⋯", key=f"dash_menu_fallback_{chart_id}", help=tooltip):
                        show_dashboard_chart_settings_dialog(chart_id, data_version)
                with c2:
                    st.caption(f"x={layout.get('x', 0)} y={layout.get('y', 0)} w={layout.get('w', 0)} h={layout.get('h', 0)}")
                    if str(cfg.get('tile_type') or 'chart') == 'quick_card':
                        _render_quick_dashboard_card(cfg.get('quick_card') or {})
                    else:
                        fig_json = item.get('figure_json') or ''
                        if fig_json:
                            try:
                                fig = pio.from_json(fig_json)
                                st.plotly_chart(fig, use_container_width=True, key=f"dash_chart_fallback_{chart_id}")
                            except Exception as e:
                                st.error(f"No se pudo renderizar el snapshot: {e}")
                        else:
                            st.warning("Este gráfico no tiene snapshot válido.")
        return

    layout_event_key = f"dash_layout_event_{current_profile}"
    grid_items = []
    for item in items:
        chart_id = str(item.get('chart_id') or '')
        layout = (item.get('config') or {}).get('layout') or {}
        grid_items.append(
            dashboard.Item(
                chart_id,
                _safe_int(layout.get('x'), 0),
                _safe_int(layout.get('y'), 0),
                _safe_int(layout.get('w'), 9),
                _safe_int(layout.get('h'), 7),
                minW=2,
                minH=3,
                resizeHandles=['se'],
            )
        )

    with elements(f"dash_elements_{current_profile}"):
        with dashboard.Grid(
            grid_items,
            cols=DASH_COLS,
            rowHeight=DASH_ROW_HEIGHT,
            margin=[DASH_GAP, DASH_GAP],
            containerPadding=[0, 0],
            compactType='vertical',
            preventCollision=False,
            isDraggable=True,
            isResizable=True,
            draggableHandle='.dash-drag-handle',
            onDragStop=sync(layout_event_key),
            onResizeStop=sync(layout_event_key),
        ):
            for item in items:
                chart_id = str(item.get('chart_id') or '')
                cfg = item.get('config') or {}
                layout = cfg.get('layout') or {}
                tile_type = str(cfg.get('tile_type') or 'chart')
                title = item.get('chart_title') or 'Tarjeta'
                updated_at = str(item.get('updated_at') or '')
                tooltip = title if not updated_at else f"{title} | Actualizado: {updated_at[:19]}"
                menu_evt_key = f"dash_menu_evt_{chart_id}"

                total_h_px = max(140, _safe_int(layout.get('h'), 7) * DASH_ROW_HEIGHT + max(0, _safe_int(layout.get('h'), 7) - 1) * DASH_GAP)
                body_h_px = max(90, total_h_px - 30)

                with mui.Paper(
                    key=chart_id,
                    elevation=0,
                    square=False,
                    sx={
                        "height": "100%",
                        "overflow": "hidden",
                        "border": "1px solid rgba(160,174,192,0.22)",
                        "borderRadius": "8px",
                        "background": "rgba(11,16,24,0.45)",
                    },
                ):
                    with mui.Box(sx={"height": "28px", "display": "flex", "alignItems": "center", "pl": "2px"}):
                        with mui.Tooltip(title=tooltip, enterDelay=1000, placement='right'):
                            mui.IconButton(
                                "⋯",
                                className='dash-drag-handle',
                                onClick=sync(menu_evt_key),
                                size='small',
                                sx={
                                    "fontSize": "0.95rem",
                                    "color": "#D9DEE7",
                                    "cursor": "grab",
                                    "minWidth": "24px",
                                    "height": "24px",
                                    "width": "24px",
                                    "p": 0,
                                },
                            )

                    if tile_type == 'quick_card':
                        card_doc = (
                            "<html><head><meta charset='utf-8'><style>html,body{margin:0;padding:0;height:100%;background:transparent;}</style></head><body>"
                            + _quick_card_html(cfg.get('quick_card') or {})
                            + "</body></html>"
                        )
                        card_src = _html_to_data_uri(card_doc)
                        html.iframe(
                            src=card_src,
                            sandbox='allow-scripts allow-same-origin',
                            css={"width": "100%", "height": f"{body_h_px}px", "border": "none", "display": "block"},
                        )
                    else:
                        fig_json = item.get('figure_json') or ''
                        if not fig_json:
                            html.div("Snapshot inválido", css={"padding": "8px", "color": "#f8d7da"})
                        else:
                            try:
                                srcdoc = _build_plotly_iframe_srcdoc(fig_json)
                                chart_src = _html_to_data_uri(srcdoc)
                                html.iframe(
                                    src=chart_src,
                                    sandbox='allow-scripts allow-same-origin',
                                    css={"width": "100%", "height": f"{body_h_px}px", "border": "none", "display": "block"},
                                )
                            except Exception as e:
                                html.div(f"No se pudo renderizar: {e}", css={"padding": "8px", "color": "#f8d7da"})

    layout_payload = _extract_layout_payload(st.session_state.get(layout_event_key))
    if layout_payload:
        st.session_state[layout_event_key] = None
        if _persist_profile_layout(current_profile, items, layout_payload):
            st.rerun()

    for item in items:
        chart_id = str(item.get('chart_id') or '')
        evt_key = f"dash_menu_evt_{chart_id}"
        if st.session_state.get(evt_key):
            st.session_state[evt_key] = None
            show_dashboard_chart_settings_dialog(chart_id, data_version)
            break


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
                overlay_on = False
                
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

                        title_vars = [str(alias_map.get(v, v)) for v in actual_chart_vars[:3]]
                        title_suffix = "..." if len(actual_chart_vars) > 3 else ""
                        chart_title = f"{chart_type} · {', '.join(title_vars)}{title_suffix}" if title_vars else f"{chart_type}"

                        bcol1, bcol2 = st.columns(2)
                        with bcol1:
                            if st.button("➕ Guardar gráfico en Dashboard", key="save_chart_snapshot_btn", use_container_width=True):
                                st.session_state.pending_dashboard_snapshot = {
                                    'chart_title': chart_title,
                                    'figure_json': fig.to_json(),
                                    'config': {
                                        'tile_type': 'chart',
                                        'layout': {'size': 'large', 'order': 0},
                                        'filters': copy.deepcopy(filters),
                                        'selected_vars': list(selected_vars),
                                        'chart_vars': list(chart_vars),
                                        'actual_chart_vars': list(actual_chart_vars),
                                        'chart_type': chart_type,
                                        'overlay_on': bool(overlay_on),
                                        'unite_vars': bool(unite_vars),
                                        'align_first': bool(align_first),
                                        'fcr_view_mode': st.session_state.get('fcr_view_mode', 'Vista general'),
                                        'pie_view_mode': st.session_state.get('pie_view_mode', 'parents'),
                                        'proyecciones_vars': list(filters.get('proyecciones_vars', [])),
                                    },
                                }
                                show_save_chart_dialog()
                        with bcol2:
                            if st.button("➕ Agregar tarjetas rápidas", key="save_quick_cards_dashboard_btn", use_container_width=True, disabled=not quick_cards):
                                st.session_state.pending_dashboard_quick_cards = copy.deepcopy(quick_cards)
                                show_save_quick_cards_dialog()
                        
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

            else:
                 st.warning("⚠️ No hay datos para la combinación de filtros seleccionada.")
                 
    elif st.session_state.current_view == "Dashboard":
        _render_dashboard_content(data_version)
    elif st.session_state.current_view == "Cards":
        _render_quick_cards_screen()

    # Empty state when NO data loaded
    if 'data_loaded' not in st.session_state or not st.session_state.data_loaded:
        st.info("Aún no hay datos cargados en la base. Usa 'Cargar / Actualizar datos' en el panel lateral.")

if __name__ == "__main__":
    main()
