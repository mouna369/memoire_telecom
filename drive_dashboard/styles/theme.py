import streamlit as st

def inject_theme(dark_mode: bool):
    if dark_mode:
        bg_main = "#0F1117"
        bg_card = "#1A1D2E"
        bg_sidebar = "#151829"
        text_primary = "#FFFFFF"
        text_secondary = "#8B92A5"
        text_muted = "#5C6178"
        border_color = "#252A3D"
        input_bg = "#1E2235"
        hover_bg = "#252A3D"
        shadow = "0 4px 24px rgba(0,0,0,0.4)"
        tag_bg = "#252A3D"
    else:
        bg_main = "#F0F2FF"
        bg_card = "#FFFFFF"
        bg_sidebar = "#1A1D2E"
        text_primary = "#1A1D2E"
        text_secondary = "#5C6178"
        text_muted = "#8B92A5"
        border_color = "#E8EAF2"
        input_bg = "#F5F6FA"
        hover_bg = "#F0F2FF"
        shadow = "0 4px 24px rgba(0,0,0,0.08)"
        tag_bg = "#F0F2FF"

    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

    :root {{
        --bg-main: {bg_main};
        --bg-card: {bg_card};
        --bg-sidebar: {bg_sidebar};
        --text-primary: {text_primary};
        --text-secondary: {text_secondary};
        --text-muted: {text_muted};
        --border: {border_color};
        --input-bg: {input_bg};
        --hover-bg: {hover_bg};
        --shadow: {shadow};
        --tag-bg: {tag_bg};
        --accent: #4F6EF7;
        --accent-light: rgba(79,110,247,0.12);
        --red: #F05454;
        --blue: #4F6EF7;
        --yellow: #F5A623;
        --green: #2ECC71;
        --purple: #9B59B6;
    }}

    * {{ font-family: 'Plus Jakarta Sans', sans-serif !important; box-sizing: border-box; }}

    /* Hide Streamlit chrome */
    #MainMenu, footer, header {{ visibility: hidden !important; }}
    .stDeployButton {{ display: none !important; }}
    [data-testid="stToolbar"] {{ display: none !important; }}
    [data-testid="stDecoration"] {{ display: none !important; }}
    [data-testid="stStatusWidget"] {{ display: none !important; }}

    /* Main background */
    .stApp {{
        background: var(--bg-main) !important;
    }}
    [data-testid="stAppViewContainer"] {{
        background: var(--bg-main) !important;
    }}
    [data-testid="stMain"] {{
        background: var(--bg-main) !important;
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: var(--bg-sidebar) !important;
        border-right: 1px solid rgba(255,255,255,0.05) !important;
        width: 220px !important;
    }}
    [data-testid="stSidebar"] > div {{
        background: var(--bg-sidebar) !important;
        padding: 0 !important;
    }}

    /* Buttons - remove defaults */
    .stButton > button {{
        border: none !important;
        background: transparent !important;
        color: var(--text-secondary) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        width: 100%;
        text-align: left;
        padding: 10px 20px !important;
        border-radius: 10px !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        transition: all 0.2s ease !important;
        cursor: pointer !important;
    }}
    .stButton > button:hover {{
        background: rgba(79,110,247,0.1) !important;
        color: #4F6EF7 !important;
    }}

    /* Main content area */
    [data-testid="stMainBlockContainer"] {{
        padding: 0 !important;
        max-width: 100% !important;
    }}

    /* Remove block padding */
    .block-container {{
        padding: 0 !important;
        max-width: 100% !important;
    }}

    /* Scrollbar */
    ::-webkit-scrollbar {{ width: 4px; height: 4px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}
    ::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 4px; }}

    /* Metric cards */
    [data-testid="stMetric"] {{
        background: var(--bg-card) !important;
        border-radius: 16px !important;
        padding: 20px !important;
        border: 1px solid var(--border) !important;
        box-shadow: var(--shadow) !important;
    }}
    [data-testid="stMetricLabel"] {{ color: var(--text-secondary) !important; font-size: 13px !important; }}
    [data-testid="stMetricValue"] {{ color: var(--text-primary) !important; font-size: 28px !important; font-weight: 700 !important; }}
    [data-testid="stMetricDelta"] {{ font-size: 12px !important; }}

    /* Plotly charts transparent */
    .js-plotly-plot .plotly .bg {{ fill: transparent !important; }}
    .stPlotlyChart {{ background: transparent !important; }}

    /* Hide expand icons */
    [data-testid="StyledFullScreenButton"] {{ display: none !important; }}

    /* Divider */
    hr {{ border-color: var(--border) !important; margin: 8px 0 !important; }}

    /* Columns gap */
    [data-testid="stHorizontalBlock"] {{ gap: 16px !important; }}

    /* Progress bar */
    .stProgress > div > div {{
        background: var(--accent) !important;
        border-radius: 4px !important;
    }}
    .stProgress > div {{
        background: var(--border) !important;
        border-radius: 4px !important;
        height: 6px !important;
    }}

    /* Input */
    .stTextInput > div > div > input {{
        background: var(--input-bg) !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        color: var(--text-primary) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }}

    /* Selectbox */
    .stSelectbox > div > div {{
        background: var(--input-bg) !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        color: var(--text-primary) !important;
    }}

    /* Checkbox */
    .stCheckbox label {{ color: var(--text-primary) !important; }}

    /* Toggle */
    .stToggle label {{ color: var(--text-primary) !important; }}
    </style>
    """, unsafe_allow_html=True)
