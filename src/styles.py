"""
Styles & Branding Module
Handles CSS injection and logo rendering for the Cermaq Dashboard.
"""
import streamlit as st
import base64
from pathlib import Path

# Project root: dashboard/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ASSETS_DIR = PROJECT_ROOT / "assets"
LOGO_FILENAME = "logov2.png"


def _load_logo_b64() -> str | None:
    """Load the logo image as a base64 string."""
    logo_path = ASSETS_DIR / LOGO_FILENAME
    if not logo_path.exists():
        print(f"[styles] Logo not found at: {logo_path}")
        return None
    return base64.b64encode(logo_path.read_bytes()).decode()


def inject_styles():
    """Inject all custom CSS including the logo shine animation."""
    st.markdown("""
        <style>
        /* General App Theme */
        .stApp {
            background-color: #0F1117;
            color: #FAFAFA;
            font-family: 'Inter', sans-serif;
        }
        /* Remove Streamlit default top padding */
        .block-container {
            padding-top: 2.5rem !important;
        }
        header[data-testid="stHeader"] {
            background: transparent !important;
        }
        
        /* Sidebar Styling */
        [data-testid="stSidebar"] {
            background-color: #161920;
            border-right: 1px solid #2B303B;
        }
        
        /* KPI Card Styling */
        div[data-testid="stMetric"] {
            background-color: #1A1D24;
            border: 1px solid #2B303B;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        [data-testid="stMetricLabel"] {
            color: #A0AEC0;
            font-size: 0.9rem;
        }
        [data-testid="stMetricValue"] {
            color: #FAFAFA;
            font-size: 1.8rem;
            font-weight: 700;
        }
        
        /* Custom FCR Sidebar Button */
        div[data-testid="stSidebar"] button[kind="primary"] {
            background-color: #1b5e20 !important;
            border-color: #1b5e20 !important;
            color: white !important;
            font-weight: 600 !important;
            width: 100% !important;
            margin-top: 10px;
        }
        div[data-testid="stSidebar"] button[kind="secondary"] {
            width: 100% !important;
            margin-top: 10px;
        }
        
        /* Buttons */
        div.stButton > button {
            background-color: #00B4D8;
            color: white;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            transition: all 0.2s;
        }
        div.stButton > button:hover {
            background-color: #0096C7;
            border-color: #0096C7;
        }
        
        /* Status Info Messages */
        .stAlert {
            background-color: #1A1D24;
            color: #FAFAFA;
            border: 1px solid #2B303B;
        }
        
        /* Table Styling */
        [data-testid="stDataFrame"] {
            border: 1px solid #2B303B;
            border-radius: 8px;
        }
        
        /* Glass Card: make inner streamlit containers transparent */
        .glass-card-zone [data-testid="stVerticalBlock"],
        .glass-card-zone [data-testid="stHorizontalBlock"],
        .glass-card-zone .stButton > button {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
        }
        .glass-card-zone .stButton > button {
            color: #555 !important;
            font-size: 0.85rem !important;
            min-height: 0 !important;
            padding: 2px 6px !important;
            line-height: 1 !important;
            border-radius: 50% !important;
            transition: color 0.2s ease, background 0.2s ease !important;
        }
        .glass-card-zone .stButton > button:hover {
            color: #FFF !important;
            background: rgba(255,255,255,0.1) !important;
        }

        /* ===== Logo + Shine Animation ===== */
        .logo-container {
            display: inline-block;
            margin-top: -8px;
            pointer-events: none;
        }
        .logo-shine {
            position: relative;
            display: inline-block;
            overflow: hidden;
            border-radius: 6px;
        }
        .logo-shine img {
            display: block;
            height: 80px;
            width: auto;
            filter: drop-shadow(0 2px 8px rgba(0, 180, 216, 0.18));
        }
        .logo-shine::after {
            content: '';
            position: absolute;
            top: -50%;
            left: -60%;
            width: 40%;
            height: 200%;
            background: linear-gradient(
                105deg,
                transparent 30%,
                rgba(255,255,255,0.06) 38%,
                rgba(255,255,255,0.18) 44%,
                rgba(255,255,255,0.28) 50%,
                rgba(255,255,255,0.18) 56%,
                rgba(255,255,255,0.06) 62%,
                transparent 70%
            );
            transform: skewX(-18deg);
            animation: logo-sweep 10s ease-in-out infinite;
        }
        @keyframes logo-sweep {
            0%   { left: -60%; opacity: 0; }
            3%   { opacity: 1; }
            12%  { left: 140%; opacity: 1; }
            15%  { opacity: 0; }
            100% { left: 140%; opacity: 0; }
        }

        /* ===== Full-Screen Loading Overlay ===== */
        .loading-overlay {
            position: fixed;
            top: 0; left: 0;
            width: 100vw; height: 100vh;
            background: rgba(0, 0, 0, 0.92);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            z-index: 9999999;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            animation: overlay-fade-in 0.4s ease-out;
        }
        @keyframes overlay-fade-in {
            from { opacity: 0; }
            to   { opacity: 1; }
        }
        .loading-overlay .logo-shine img {
            height: 180px;
        }
        .loading-text {
            margin-top: 32px;
            display: flex;
            align-items: center;
            gap: 10px;
            color: #A0AEC0;
            font-family: 'Inter', sans-serif;
            font-size: 0.95rem;
            letter-spacing: 0.3px;
        }
        .loading-spinner {
            width: 18px; height: 18px;
            border: 2px solid rgba(255,255,255,0.15);
            border-top-color: #4ECDC4;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        /* Closing animation: logo shrinks & moves to top-left */
        .loading-overlay.closing {
            animation: overlay-fade-out 0.8s ease-in-out forwards;
        }
        .loading-overlay.closing .logo-shine {
            animation: logo-fly-home 0.8s ease-in-out forwards;
        }
        .loading-overlay.closing .loading-text {
            animation: text-fade 0.3s ease-out forwards;
        }
        @keyframes overlay-fade-out {
            0%   { opacity: 1; }
            70%  { opacity: 1; }
            100% { opacity: 0; }
        }
        @keyframes logo-fly-home {
            0% {
                transform: translate(0, 0) scale(1);
            }
            100% {
                transform: translate(calc(-50vw + 120px), calc(-50vh + 50px)) scale(0.29);
            }
        }
        /* Back Arrow Button styling (Dashboard view) */
        button[data-testid="baseButton-secondary"]:has(div:contains("⬅️")) {
            background-color: transparent !important;
            border: none !important;
            box-shadow: none !important;
            font-size: 1.5rem !important;
            padding: 0 !important;
            color: #A0AEC0 !important;
            transition: color 0.2s;
        }
        button[data-testid="baseButton-secondary"]:has(div:contains("⬅️")):hover {
            color: #4ECDC4 !important;
            background-color: transparent !important;
        }
        </style>
    """, unsafe_allow_html=True)


def inject_logo(dashboard_mode=False):
    """Render the Cermaq logo at top-left. Slimmer if dashboard_mode is True."""
    logo_b64 = _load_logo_b64()
    if logo_b64:
        mask_css = (
            f"-webkit-mask-image: url(data:image/png;base64,{logo_b64}); "
            f"mask-image: url(data:image/png;base64,{logo_b64}); "
            f"-webkit-mask-size: contain; mask-size: contain; "
            f"-webkit-mask-repeat: no-repeat; mask-repeat: no-repeat; "
            f"-webkit-mask-position: center; mask-position: center;"
        )
        
        # Adjust styles based on dashboard mode for a slimmer header
        container_style = "margin-bottom: -15px;" if dashboard_mode else ""
        shine_style = f"{mask_css}; height: 45px !important;" if dashboard_mode else mask_css
        
        st.markdown(
            f'<div class="logo-container" style="{container_style}">'
            f'<div class="logo-shine" style="{shine_style}">'
            f'<img src="data:image/png;base64,{logo_b64}" alt="Cermaq">'
            f'</div></div>',
            unsafe_allow_html=True
        )
    else:
        print("[styles] Logo skipped — file not found")


def show_loading_screen(message: str = "Limpiando y consolidando datos..."):
    """Show a full-screen loading overlay with centered logo and spinner text."""
    logo_b64 = _load_logo_b64()
    if not logo_b64:
        return None
    
    mask_css = (
        f"-webkit-mask-image: url(data:image/png;base64,{logo_b64}); "
        f"mask-image: url(data:image/png;base64,{logo_b64}); "
        f"-webkit-mask-size: contain; mask-size: contain; "
        f"-webkit-mask-repeat: no-repeat; mask-repeat: no-repeat; "
        f"-webkit-mask-position: center; mask-position: center;"
    )
    
    placeholder = st.empty()
    placeholder.markdown(
        f'<div class="loading-overlay">'
        f'  <div class="logo-shine" style="{mask_css}">'
        f'    <img src="data:image/png;base64,{logo_b64}" alt="Cermaq">'
        f'  </div>'
        f'  <div class="loading-text">'
        f'    <div class="loading-spinner"></div>'
        f'    {message}'
        f'  </div>'
        f'</div>',
        unsafe_allow_html=True
    )
    return placeholder


def hide_loading_screen(placeholder):
    """Animate the loading overlay closing (logo flies to header position), then remove."""
    import time
    if not placeholder:
        return
    logo_b64 = _load_logo_b64()
    if not logo_b64:
        placeholder.empty()
        return
    
    mask_css = (
        f"-webkit-mask-image: url(data:image/png;base64,{logo_b64}); "
        f"mask-image: url(data:image/png;base64,{logo_b64}); "
        f"-webkit-mask-size: contain; mask-size: contain; "
        f"-webkit-mask-repeat: no-repeat; mask-repeat: no-repeat; "
        f"-webkit-mask-position: center; mask-position: center;"
    )
    
    # Replace with closing animation version
    placeholder.markdown(
        f'<div class="loading-overlay closing">'
        f'  <div class="logo-shine" style="{mask_css}">'
        f'    <img src="data:image/png;base64,{logo_b64}" alt="Cermaq">'
        f'  </div>'
        f'  <div class="loading-text">'
        f'    <div class="loading-spinner"></div>'
        f'    Listo'
        f'  </div>'
        f'</div>',
        unsafe_allow_html=True
    )
    time.sleep(0.9)  # Wait for animation to finish
    placeholder.empty()

def show_view_transition():
    """Show a quick full-screen sweep animation when changing views."""
    import time
    logo_b64 = _load_logo_b64()
    if not logo_b64:
        return
        
    mask_css = (
        f"-webkit-mask-image: url(data:image/png;base64,{logo_b64}); "
        f"mask-image: url(data:image/png;base64,{logo_b64}); "
        f"-webkit-mask-size: contain; mask-size: contain; "
        f"-webkit-mask-repeat: no-repeat; mask-repeat: no-repeat; "
        f"-webkit-mask-position: center; mask-position: center;"
    )

    # CSS for the transition
    css = """
    <style>
    .view-transition-overlay {
        position: fixed;
        top: 0; left: 0; width: 100vw; height: 100vh;
        background: rgba(15, 23, 42, 0.95);
        backdrop-filter: blur(8px);
        z-index: 999999;
        display: flex;
        justify-content: center;
        align-items: center;
        animation: transition-fade 0.7s cubic-bezier(0.4, 0, 0.2, 1) forwards;
        pointer-events: none;
    }
    .view-transition-overlay .logo-shine {
        width: 180px;
        height: 180px;
        position: relative;
        background: linear-gradient(-45deg, #1A202C 40%, #4ECDC4 50%, #1A202C 60%);
        background-size: 200% auto;
        animation: shine 0.6s linear infinite;
    }
    .view-transition-overlay img {
        width: 100%;
        height: 100%;
        object-fit: contain;
        visibility: hidden;
    }
    @keyframes transition-fade {
        0% { opacity: 0; backdrop-filter: blur(0px); }
        30% { opacity: 1; backdrop-filter: blur(12px); }
        70% { opacity: 1; backdrop-filter: blur(12px); }
        100% { opacity: 0; backdrop-filter: blur(0px); }
    }
    </style>
    """
    
    html = f"""
    {css}
    <div class="view-transition-overlay">
        <div class="logo-shine" style="{mask_css}">
            <img src="data:image/png;base64,{logo_b64}" alt="Cermaq">
        </div>
    </div>
    """
    
    placeholder = st.empty()
    placeholder.markdown(html, unsafe_allow_html=True)
    time.sleep(0.75) # Wait precisely for the CSS animation to complete
    placeholder.empty()
