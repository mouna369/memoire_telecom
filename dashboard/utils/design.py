# utils/design.py
import streamlit as st

def apply_custom_design():
    """Applique le design moderne light/blanc inspiré du dashboard Analytics."""
    st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>

    /* ── RESET GLOBAL ── */
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }

    /* ── BACKGROUND ── */
    [data-testid="stAppViewContainer"] {
        background: #F4F6FB !important;
    }
    [data-testid="stHeader"] {
        background: transparent !important;
    }
    .main .block-container {
        padding: 1.5rem 2rem 2rem 2rem !important;
        max-width: 1400px !important;
    }

    /* ── SIDEBAR ── */
    [data-testid="stSidebar"] {
        background: #FFFFFF !important;
        border-right: 1px solid #E8ECF4 !important;
        box-shadow: 4px 0 24px rgba(0,0,0,0.04) !important;
    }
    [data-testid="stSidebar"] .stRadio label {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
        color: #64748B !important;
        padding: 0.5rem 0.75rem !important;
        border-radius: 10px !important;
        transition: all 0.2s !important;
        display: block !important;
        margin: 2px 0 !important;
    }
    [data-testid="stSidebar"] .stRadio label:hover {
        background: #F1F5FF !important;
        color: #FF6B35 !important;
    }
    [data-testid="stSidebar"] .stRadio [data-checked="true"] label,
    [data-testid="stSidebar"] .stRadio input:checked + label {
        background: #FFF0EB !important;
        color: #FF6B35 !important;
        font-weight: 600 !important;
    }

    /* ── TITLES ── */
    h1, h2, h3 {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        color: #1E2A3B !important;
        font-weight: 800 !important;
        letter-spacing: -0.02em !important;
    }
    h1 { font-size: 1.75rem !important; }
    h2 { font-size: 1.3rem !important; }
    h3 { font-size: 1.05rem !important; }

    /* ── METRIC CARDS ── */
    [data-testid="stMetric"] {
        background: #FFFFFF !important;
        border-radius: 16px !important;
        padding: 1.25rem 1.5rem !important;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06) !important;
        border: 1px solid #F0F2F8 !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.78rem !important;
        color: #94A3B8 !important;
        font-weight: 500 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.06em !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.9rem !important;
        font-weight: 800 !important;
        color: #1E2A3B !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.82rem !important;
        font-weight: 600 !important;
    }

    /* ── BUTTONS ── */
    .stButton > button {
        background: #FF6B35 !important;
        color: #FFFFFF !important;
        border: none !important;
        border-radius: 12px !important;
        font-weight: 600 !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
        padding: 0.6rem 1.4rem !important;
        box-shadow: 0 4px 14px rgba(255,107,53,0.35) !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 6px 20px rgba(255,107,53,0.45) !important;
    }

    /* ── SELECTBOX / MULTISELECT ── */
    [data-testid="stSelectbox"] > div > div,
    [data-testid="stMultiSelect"] > div > div {
        background: #FFFFFF !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 12px !important;
        font-size: 0.88rem !important;
        color: #1E2A3B !important;
    }

    /* ── TEXT INPUT ── */
    [data-testid="stTextInput"] input,
    [data-testid="stTextArea"] textarea,
    [data-testid="stChatInput"] textarea {
        background: #FFFFFF !important;
        border: 1.5px solid #E2E8F0 !important;
        border-radius: 12px !important;
        color: #1E2A3B !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.9rem !important;
    }
    [data-testid="stTextInput"] input:focus,
    [data-testid="stChatInput"] textarea:focus {
        border-color: #FF6B35 !important;
        box-shadow: 0 0 0 3px rgba(255,107,53,0.15) !important;
    }

    /* ── DATAFRAME ── */
    [data-testid="stDataFrame"] {
        border-radius: 16px !important;
        overflow: hidden !important;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06) !important;
        border: 1px solid #F0F2F8 !important;
    }

    /* ── EXPANDER ── */
    [data-testid="stExpander"] {
        background: #FFFFFF !important;
        border: 1px solid #E8ECF4 !important;
        border-radius: 16px !important;
    }

    /* ── ALERTS ── */
    .stSuccess, .stInfo, .stWarning, .stError {
        border-radius: 12px !important;
        font-size: 0.88rem !important;
    }

    /* ── DOWNLOAD BUTTON ── */
    [data-testid="stDownloadButton"] > button {
        background: #1E2A3B !important;
        box-shadow: 0 4px 14px rgba(30,42,59,0.25) !important;
    }
    [data-testid="stDownloadButton"] > button:hover {
        box-shadow: 0 6px 20px rgba(30,42,59,0.35) !important;
    }

    /* ── PLOTLY CHARTS ── */
    .js-plotly-plot .plotly {
        border-radius: 16px !important;
    }

    /* ── SCROLLBAR ── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #F4F6FB; }
    ::-webkit-scrollbar-thumb { background: #CBD5E1; border-radius: 10px; }
    ::-webkit-scrollbar-thumb:hover { background: #FF6B35; }

    /* ── SIDEBAR NAV BADGE ── */
    .nav-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: #FF6B35;
        color: white;
        font-size: 0.65rem;
        font-weight: 700;
        width: 18px;
        height: 18px;
        border-radius: 50%;
        float: right;
        margin-top: 1px;
    }

    /* ── PAGE HEADER ── */
    .page-header {
        background: linear-gradient(135deg, #FFFFFF 0%, #FFF8F5 100%);
        border-radius: 20px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.5rem;
        border: 1px solid #FFE4D9;
        box-shadow: 0 2px 16px rgba(255,107,53,0.08);
    }
    .page-header h1 {
        margin: 0 0 0.25rem 0 !important;
        font-size: 1.6rem !important;
    }
    .page-header p {
        color: #94A3B8;
        margin: 0;
        font-size: 0.88rem;
    }

    /* ── KPI CARD CUSTOM ── */
    .kpi-card {
        background: #FFFFFF;
        border-radius: 18px;
        padding: 1.4rem 1.5rem;
        box-shadow: 0 2px 16px rgba(0,0,0,0.06);
        border: 1px solid #F0F2F8;
        height: 100%;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 28px rgba(0,0,0,0.1);
    }
    .kpi-icon {
        width: 44px;
        height: 44px;
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.3rem;
        margin-bottom: 1rem;
    }
    .kpi-label {
        font-size: 0.75rem;
        font-weight: 600;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        margin-bottom: 0.3rem;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 800;
        color: #1E2A3B;
        line-height: 1;
        margin-bottom: 0.5rem;
    }
    .kpi-delta {
        font-size: 0.78rem;
        font-weight: 600;
        padding: 3px 8px;
        border-radius: 20px;
        display: inline-block;
    }

    /* ── COMMENT CARD ── */
    .comment-card {
        background: #FFFFFF;
        border-radius: 14px;
        padding: 1rem 1.25rem;
        margin: 0.6rem 0;
        border: 1px solid #F0F2F8;
        box-shadow: 0 1px 6px rgba(0,0,0,0.04);
        transition: box-shadow 0.2s;
    }
    .comment-card:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.08);
    }

    /* ── SECTION TITLE ── */
    .section-title {
        font-size: 1rem;
        font-weight: 700;
        color: #1E2A3B;
        margin: 1.5rem 0 1rem 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }

    /* ── STAT ROW ── */
    .stat-row {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 0.6rem 0;
        border-bottom: 1px solid #F4F6FB;
    }
    .stat-row:last-child { border-bottom: none; }

    </style>
    """, unsafe_allow_html=True)


def kpi_card(icon, icon_bg, label, value, delta=None, delta_positive=True):
    delta_color = "#22C55E" if delta_positive else "#EF4444"
    delta_bg = "rgba(34,197,94,0.12)" if delta_positive else "rgba(239,68,68,0.12)"
    delta_html = f'<span class="kpi-delta" style="background:{delta_bg};color:{delta_color};">{delta}</span>' if delta else ""
    return f"""
    <div class="kpi-card">
        <div class="kpi-icon" style="background:{icon_bg};">{icon}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """


def page_header(icon, title, subtitle):
    st.markdown(f"""
    <div class="page-header">
        <h1>{icon} {title}</h1>
        <p>{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)


def sidebar_logo():
    st.markdown("""
    <div style="padding: 1.5rem 1rem 1rem 1rem; border-bottom: 1px solid #F0F2F8; margin-bottom: 1rem;">
        <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem;">
            <div style="width: 42px; height: 42px; background: linear-gradient(135deg, #FF6B35, #FF9A6C);
                        border-radius: 12px; display: flex; align-items: center; justify-content: center;
                        font-size: 1.3rem; box-shadow: 0 4px 12px rgba(255,107,53,0.3);">📡</div>
            <div>
                <div style="font-weight: 800; font-size: 1.1rem; color: #1E2A3B; letter-spacing: -0.01em;">TÉLÉCOM DZ</div>
                <div style="font-size: 0.7rem; color: #94A3B8; font-weight: 500;">Analyse des sentiments</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def plotly_layout_light():
    """Config Plotly pour un thème light cohérent."""
    return dict(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Plus Jakarta Sans', color='#475569', size=12),
        title_font=dict(color='#1E2A3B', size=15, family='Plus Jakarta Sans'),
        xaxis=dict(gridcolor='#F1F5F9', linecolor='#E2E8F0', tickfont=dict(color='#94A3B8', size=11)),
        yaxis=dict(gridcolor='#F1F5F9', linecolor='#E2E8F0', tickfont=dict(color='#94A3B8', size=11)),
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(
            bgcolor='rgba(255,255,255,0.8)',
            bordercolor='#E2E8F0',
            borderwidth=1,
            font=dict(color='#475569', size=11)
        )
    )


SENTIMENT_COLORS = {
    'positif': '#22C55E',
    'negatif': '#EF4444',
    'neutre': '#F59E0B',
    'mixed': '#8B5CF6'
}