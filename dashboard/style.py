# 📁 style.py - Thème centralisé pour tout le dashboard
# Palette: 🔵 Bleu #2563eb • 🟢 Vert #22c55e • ⚪ Blanc #ffffff • ⚫ Noir #0f172a

THEME_CSS = """
<style>
/* ===== VARIABLES CSS ===== */
:root {
    /* Palette principale */
    --blue-500: #2563eb;
    --blue-600: #1d4ed8;
    --blue-700: #1e40af;
    --green-500: #22c55e;
    --green-600: #16a34a;
    --white: #ffffff;
    --black: #0f172a;
    
    /* Surfaces */
    --bg-primary: var(--white);
    --bg-secondary: #f8fafc;
    --bg-tertiary: #f1f5f9;
    --bg-dark: var(--black);
    
    /* Texte */
    --text-primary: #1e293b;
    --text-secondary: #64748b;
    --text-muted: #94a3b8;
    --text-inverse: var(--white);
    
    /* Bordures */
    --border-light: #e2e8f0;
    --border-medium: #cbd5e1;
    --border-dark: #64748b;
    
    /* Ombres */
    --shadow-sm: 0 1px 2px rgba(0,0,0,0.05);
    --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.1);
    --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1);
    --shadow-blue: 0 4px 14px rgba(37, 99, 235, 0.15);
    --shadow-green: 0 4px 14px rgba(34, 197, 94, 0.15);
    
    /* Rayons */
    --radius-sm: 8px;
    --radius-md: 12px;
    --radius-lg: 16px;
    --radius-xl: 24px;
    
    /* Transitions */
    --transition-fast: 150ms ease;
    --transition-normal: 250ms ease;
}

/* ===== BASE ===== */
.stApp {
    background: var(--bg-primary) !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}

/* ===== SIDEBAR ===== */
[data-testid="stSidebar"] {
    background: var(--bg-secondary) !important;
    border-right: 1px solid var(--border-light) !important;
}
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] .stButton > button {
    color: var(--text-primary) !important;
}
[data-testid="stSidebar"] hr {
    border-color: var(--border-light) !important;
}

/* ===== HEADER & TITRES ===== */
.page-header {
    background: linear-gradient(135deg, var(--blue-500), var(--blue-700));
    border-radius: var(--radius-lg);
    padding: 1.5rem 2rem;
    margin-bottom: 1.5rem;
    color: var(--text-inverse);
    position: relative;
    overflow: hidden;
}
.page-header::before {
    content: '';
    position: absolute;
    top: -50%; right: -10%;
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(255,255,255,0.15), transparent 70%);
    border-radius: 50%;
}
.page-header h1 {
    margin: 0; font-size: 1.8rem; font-weight: 700;
}
.page-header p {
    margin: 0.3rem 0 0; opacity: 0.9; font-size: 0.95rem;
}

/* ===== CARTES KPI ===== */
.kpi-card {
    background: var(--bg-primary);
    border: 1px solid var(--border-light);
    border-radius: var(--radius-md);
    padding: 1rem 1.2rem;
    box-shadow: var(--shadow-sm);
    transition: transform var(--transition-fast), box-shadow var(--transition-fast);
    border-top: 3px solid var(--blue-500);
    height: 100%;
}
.kpi-card:hover {
    transform: translateY(-3px);
    box-shadow: var(--shadow-md);
}
.kpi-card.green { border-top-color: var(--green-500); }
.kpi-card.red { border-top-color: #ef4444; }
.kpi-card.yellow { border-top-color: #f59e0b; }
.kpi-card.purple { border-top-color: #8b5cf6; }

.kpi-icon { font-size: 1.5rem; margin-bottom: 0.3rem; }
.kpi-label {
    font-size: 0.75rem; color: var(--text-secondary);
    text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600;
}
.kpi-value {
    font-size: 1.6rem; font-weight: 700; color: var(--text-primary);
    margin: 0.2rem 0; line-height: 1.2;
}
.kpi-delta {
    font-size: 0.85rem; font-weight: 600;
    display: inline-flex; align-items: center; gap: 0.2rem;
}
.kpi-delta.up { color: var(--green-600); }
.kpi-delta.down { color: #ef4444; }

/* ===== CARTES GÉNÉRIQUES ===== */
.card {
    background: var(--bg-primary);
    border: 1px solid var(--border-light);
    border-radius: var(--radius-md);
    padding: 1.2rem;
    margin: 0.5rem 0;
    box-shadow: var(--shadow-sm);
}
.card-title {
    font-size: 1rem; font-weight: 600; color: var(--text-primary);
    margin: 0 0 0.8rem; padding-bottom: 0.5rem;
    border-bottom: 2px solid var(--blue-500);
    display: flex; align-items: center; gap: 0.5rem;
}

/* ===== COMMENTAIRES ===== */
.comment-card {
    background: var(--bg-secondary);
    border-radius: var(--radius-md);
    padding: 1rem 1.2rem;
    margin: 0.6rem 0;
    border-left: 4px solid var(--green-500);
    transition: box-shadow var(--transition-fast);
}
.comment-card:hover { box-shadow: var(--shadow-md); }
.comment-card.neg { border-left-color: #ef4444; }
.comment-card.neu { border-left-color: #f59e0b; }

.comment-label {
    font-size: 0.8rem; font-weight: 700; text-transform: uppercase;
    display: inline-flex; align-items: center; gap: 0.3rem;
}
.comment-meta {
    font-size: 0.75rem; color: var(--text-muted);
}
.comment-text {
    color: var(--text-primary); line-height: 1.5; margin: 0.5rem 0;
    font-size: 0.9rem;
}

/* ===== BOUTONS ===== */
.stButton > button {
    background: var(--blue-500) !important;
    color: var(--white) !important;
    border: none !important;
    border-radius: var(--radius-sm) !important;
    padding: 0.5rem 1.2rem !important;
    font-weight: 500 !important;
    font-size: 0.9rem !important;
    transition: all var(--transition-fast) !important;
    box-shadow: var(--shadow-sm) !important;
}
.stButton > button:hover {
    background: var(--blue-600) !important;
    transform: translateY(-1px) !important;
    box-shadow: var(--shadow-blue) !important;
}
.stButton > button:active {
    transform: translateY(0) !important;
}

/* ===== FILTRES SIDEBAR ===== */
.filter-section {
    background: var(--bg-tertiary);
    padding: 0.8rem;
    border-radius: var(--radius-sm);
    margin-bottom: 0.8rem;
}
.filter-label {
    font-size: 0.75rem; color: var(--text-secondary);
    font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 0.4rem; display: block;
}

/* ===== GRAPHIQUES ===== */
.chart-container {
    background: var(--bg-primary);
    border: 1px solid var(--border-light);
    border-radius: var(--radius-md);
    padding: 1rem;
    margin: 0.5rem 0;
}

/* ===== TABLEAUX ===== */
.dataframe {
    border-radius: var(--radius-sm) !important;
    overflow: hidden !important;
    border: 1px solid var(--border-light) !important;
    font-size: 0.85rem !important;
}
.dataframe thead {
    background: var(--bg-secondary) !important;
}
.dataframe tbody tr:hover {
    background: var(--bg-tertiary) !important;
}

/* ===== EXPANDERS ===== */
.streamlit-expanderHeader {
    background: var(--bg-secondary) !important;
    border-radius: var(--radius-sm) !important;
    border: 1px solid var(--border-light) !important;
    font-weight: 600 !important;
}
.streamlit-expanderContent {
    background: var(--bg-primary) !important;
    border: 1px solid var(--border-light) !important;
    border-radius: 0 0 var(--radius-sm) var(--radius-sm) !important;
    padding: 1rem !important;
}

/* ===== ALERTES ===== */
.stInfo, .stSuccess, .stWarning, .stError {
    border-radius: var(--radius-sm) !important;
    border-left: 4px solid var(--blue-500) !important;
}
.stSuccess { border-left-color: var(--green-500) !important; }
.stWarning { border-left-color: #f59e0b !important; }
.stError { border-left-color: #ef4444 !important; }

/* ===== UTILITIES ===== */
.text-center { text-align: center; }
.mb-1 { margin-bottom: 0.5rem; }
.mb-2 { margin-bottom: 1rem; }
.mt-1 { margin-top: 0.5rem; }
.mt-2 { margin-top: 1rem; }
.flex { display: flex; }
.items-center { align-items: center; }
.justify-between { justify-content: space-between; }
.gap-2 { gap: 0.5rem; }

/* ===== ANIMATIONS ===== */
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
}
.fade-in { animation: fadeIn 0.3s ease-out; }

/* ===== SCROLLBAR ===== */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb {
    background: var(--border-medium);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover { background: var(--text-secondary); }

/* ===== INPUTS ===== */
input, textarea, select {
    border-radius: var(--radius-sm) !important;
    border: 1px solid var(--border-light) !important;
    background: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}
input:focus, textarea:focus, select:focus {
    border-color: var(--blue-500) !important;
    box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1) !important;
    outline: none !important;
}
</style>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
"""

# Configuration Plotly harmonisée
PLOTLY_LAYOUT = {
    'plot_bgcolor': 'rgba(255,255,255,1)',
    'paper_bgcolor': 'rgba(255,255,255,1)',
    'font': dict(family='Inter, sans-serif', color='#1e293b', size=11),
    'title_font': dict(family='Inter, sans-serif', size=14, color='#1e293b'),
    'legend': dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(size=10)),
    'margin': dict(l=40, r=20, t=50, b=40),
    'hovermode': 'x unified',
    'hoverlabel': dict(bgcolor='white', font_size=11, font_family='Inter, sans-serif'),
    'xaxis': dict(gridcolor='#e2e8f0', tickfont=dict(color='#64748b', size=10)),
    'yaxis': dict(gridcolor='#e2e8f0', tickfont=dict(color='#64748b', size=10)),
}

# Mapping couleurs sentiments (palette cohérente)
COLOR_MAP = {
    'positif': '#22c55e',    # Vert
    'negatif': '#ef4444',    # Rouge (contraste nécessaire)
    'neutre': '#f59e0b',     # Orange
    'mixed': '#8b5cf6',      # Violet
}

# Helper functions
def kpi_card(icon: str, label: str, value: str, delta: str = None, 
             delta_up: bool = None, color: str = "blue") -> str:
    """Génère le HTML d'une carte KPI"""
    color_class = ""
    if color == "green": color_class = "green"
    elif color == "red": color_class = "red"
    elif color == "yellow": color_class = "yellow"
    elif color == "purple": color_class = "purple"
    
    delta_html = ""
    if delta:
        direction = "up" if delta_up else "down"
        symbol = "▲" if delta_up else "▼"
        delta_html = f'<div class="kpi-delta {direction}">{symbol} {delta}</div>'
    
    return f'''
    <div class="kpi-card {color_class}">
        <div class="kpi-icon">{icon}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    '''

def page_header(icon: str, title: str, subtitle: str = None) -> str:
    """Génère l'en-tête de page"""
    subtitle_html = f'<p>{subtitle}</p>' if subtitle else ''
    return f'''
    <div class="page-header">
        <h1>{icon} {title}</h1>
        {subtitle_html}
    </div>
    '''