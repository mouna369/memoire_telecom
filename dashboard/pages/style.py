"""
style.py — CSS global partagé par toutes les pages
"""

THEME_CSS = """
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
/* ── Reset & base ── */
html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}
.stApp { background: #0f1117 !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #13151f !important;
    border-right: 1px solid #1e2130 !important;
}
[data-testid="stSidebar"] * { color: #cbd5e1; }
[data-testid="stSidebarNav"] { padding-top: .5rem; }

/* ── Main container ── */
.main .block-container {
    padding: 1.5rem 2rem !important;
    max-width: 1400px;
}

/* ── Cards ── */
.card {
    background: #1a1d2e;
    border: 1px solid #1e2130;
    border-radius: 16px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
}
.card-title {
    font-size: .95rem;
    font-weight: 700;
    color: #f1f5f9;
    margin-bottom: 2px;
}
.card-sub {
    font-size: .73rem;
    color: #475569;
    margin-bottom: .8rem;
}

/* ── Page header ── */
.page-header {
    background: linear-gradient(135deg, #1a1d2e 0%, #0f1117 100%);
    border: 1px solid #1e2130;
    border-radius: 16px;
    padding: 1.2rem 1.6rem;
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    gap: 1rem;
}
.page-header-icon {
    width: 48px; height: 48px;
    border-radius: 14px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.4rem;
}
.page-header h1 {
    font-size: 1.4rem;
    font-weight: 800;
    color: #f1f5f9;
    margin: 0;
}
.page-header p {
    font-size: .78rem;
    color: #475569;
    margin: 2px 0 0 0;
}

/* ── KPI metric cards ── */
.kpi-card {
    background: #1a1d2e;
    border: 1px solid #1e2130;
    border-radius: 14px;
    padding: 1.1rem 1.2rem;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    border-radius: 14px 14px 0 0;
}
.kpi-card.green::before  { background: #10b981; }
.kpi-card.red::before    { background: #ef4444; }
.kpi-card.blue::before   { background: #3b82f6; }
.kpi-card.yellow::before { background: #f59e0b; }
.kpi-card.purple::before { background: #8b5cf6; }

.kpi-icon {
    width: 40px; height: 40px;
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem;
    margin-bottom: .6rem;
}
.kpi-value {
    font-size: 1.65rem;
    font-weight: 800;
    color: #f1f5f9;
    line-height: 1.1;
}
.kpi-label {
    font-size: .72rem;
    color: #475569;
    font-weight: 500;
    margin-top: 3px;
}
.kpi-delta-up   { color: #10b981; font-size: .72rem; font-weight: 600; }
.kpi-delta-down { color: #ef4444; font-size: .72rem; font-weight: 600; }

/* ── Badges ── */
.badge-up   { background:rgba(16,185,129,.15); color:#10b981; border-radius:20px; padding:2px 8px; font-size:.7rem; font-weight:600; }
.badge-down { background:rgba(239,68,68,.15);  color:#ef4444; border-radius:20px; padding:2px 8px; font-size:.7rem; font-weight:600; }
.badge-blue { background:rgba(59,130,246,.15); color:#60a5fa; border-radius:20px; padding:2px 8px; font-size:.7rem; font-weight:600; }

/* ── Comment cards ── */
.comment-card {
    background: #1a1d2e;
    border-radius: 12px;
    padding: 1rem 1.2rem;
    margin: .6rem 0;
    border-left: 4px solid;
    transition: transform .15s;
}
.comment-card:hover { transform: translateX(3px); }
.comment-card.pos { border-color: #10b981; }
.comment-card.neg { border-color: #ef4444; }
.comment-text  { color: #cbd5e1; font-size: .85rem; line-height: 1.6; margin: .4rem 0; }
.comment-meta  { color: #475569; font-size: .72rem; }
.comment-label { font-weight: 700; font-size: .78rem; }

/* ── Table styling ── */
.stDataFrame { border-radius: 12px; overflow: hidden; }
[data-testid="stDataFrame"] { border: 1px solid #1e2130; border-radius: 12px; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #13151f;
    border-radius: 10px;
    padding: 4px;
    gap: 4px;
    border: 1px solid #1e2130;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    border-radius: 7px;
    color: #475569;
    font-weight: 600;
    font-size: .8rem;
    border: none;
}
.stTabs [aria-selected="true"] {
    background: #3b82f6 !important;
    color: #fff !important;
}

/* ── Plotly override ── */
.js-plotly-plot { border-radius: 12px; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #0f1117; }
::-webkit-scrollbar-thumb { background: #1e2130; border-radius: 10px; }

/* ── Streamlit default overrides ── */
.stMetric { background: transparent !important; }
[data-testid="metric-container"] {
    background: #1a1d2e !important;
    border: 1px solid #1e2130 !important;
    border-radius: 14px !important;
    padding: 1rem !important;
}
[data-testid="stMetricValue"] { color: #f1f5f9 !important; font-weight: 800 !important; }
[data-testid="stMetricLabel"] { color: #475569 !important; }
[data-testid="stMetricDelta"] svg { display: none; }

/* ── Buttons ── */
.stButton > button {
    background: #1a1d2e !important;
    border: 1px solid #1e2130 !important;
    color: #cbd5e1 !important;
    border-radius: 10px !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    font-weight: 600 !important;
    transition: all .2s !important;
}
.stButton > button:hover {
    border-color: #3b82f6 !important;
    color: #60a5fa !important;
    background: rgba(59,130,246,.08) !important;
}

/* ── Expanders ── */
.streamlit-expanderHeader {
    background: #1a1d2e !important;
    border-radius: 10px !important;
    color: #cbd5e1 !important;
    font-weight: 600 !important;
}
.streamlit-expanderContent {
    background: #13151f !important;
    border: 1px solid #1e2130 !important;
    border-radius: 0 0 10px 10px !important;
}

/* ── Download button ── */
.stDownloadButton > button {
    background: linear-gradient(135deg,#3b82f6,#6366f1) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 700 !important;
}
</style>
"""

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Plus Jakarta Sans", size=11, color="#94a3b8"),
    title_font=dict(color="#f1f5f9", size=14, family="Plus Jakarta Sans"),
    xaxis=dict(gridcolor="#1e2130", tickfont=dict(color="#64748b"), linecolor="#1e2130"),
    yaxis=dict(gridcolor="#1e2130", tickfont=dict(color="#64748b"), zeroline=False),
    margin=dict(l=0, r=0, t=30, b=0),
    showlegend=True,
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(color="#94a3b8", size=11),
    ),
)

COLOR_MAP = {
    "positif": "#10b981",
    "negatif": "#ef4444",
    "neutre":  "#6b7280",
    "mixed":   "#f59e0b",
}

def kpi_card(icon, label, value, delta=None, delta_up=True, color="blue"):
    delta_html = ""
    if delta:
        cls = "kpi-delta-up" if delta_up else "kpi-delta-down"
        arrow = "↑" if delta_up else "↓"
        delta_html = f'<div class="{cls}">{arrow} {delta}</div>'
    bg_map = {"green":"rgba(16,185,129,.1)","red":"rgba(239,68,68,.1)",
              "blue":"rgba(59,130,246,.1)","yellow":"rgba(245,158,11,.1)","purple":"rgba(139,92,246,.1)"}
    return f"""
    <div class="kpi-card {color}">
        <div class="kpi-icon" style="background:{bg_map.get(color,'rgba(59,130,246,.1)')}">{icon}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {delta_html}
    </div>"""


def page_header(icon, title, subtitle, bg="#3b82f6"):
    return f"""
    <div class="page-header">
        <div class="page-header-icon" style="background:rgba(59,130,246,.15)">{icon}</div>
        <div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
    </div>"""