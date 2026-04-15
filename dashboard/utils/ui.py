# 📁 utils/ui.py
# Composants UI réutilisables pour le Dashboard Télécom Algérie
# Palette : 🔵 Bleu #2563eb • 🟢 Vert #22c55e • ⚪ Blanc • ⚫ Noir

import streamlit as st

# ============================================
# CSS GLOBAL
# ============================================
def inject_custom_css():
    """Injecte le CSS personnalisé pour harmoniser le design"""
    st.markdown("""
    <style>
    /* ===== VARIABLES CSS ===== */
    :root {
        --primary: #2563eb;
        --primary-hover: #1d4ed8;
        --success: #22c55e;
        --background: #ffffff;
        --surface: #f8fafc;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --border: #e2e8f0;
        --shadow: 0 1px 3px rgba(0,0,0,0.1);
        --radius: 12px;
    }
    
    /* ===== BASE ===== */
    .stApp {
        background: var(--background) !important;
        color: var(--text-primary) !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    
    /* ===== SIDEBAR ===== */
    [data-testid="stSidebar"] {
        background: var(--surface) !important;
        border-right: 1px solid var(--border) !important;
    }
    [data-testid="stSidebar"] .stMarkdown, 
    [data-testid="stSidebar"] .stButton > button {
        color: var(--text-primary) !important;
    }
    
    /* ===== CARTES KPI ===== */
    .kpi-card {
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 1.2rem;
        box-shadow: var(--shadow);
        transition: transform 0.2s, box-shadow 0.2s;
        border-left: 4px solid var(--primary);
        height: 100%;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.15);
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--text-primary);
        margin: 0.3rem 0;
        line-height: 1.2;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 500;
    }
    .kpi-delta {
        font-size: 0.9rem;
        font-weight: 600;
        margin-top: 0.3rem;
    }
    .kpi-delta.positive { color: var(--success); }
    .kpi-delta.negative { color: #ef4444; }
    
    /* ===== BOUTONS ===== */
    .stButton > button {
        background: var(--primary) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.5rem 1.2rem !important;
        font-weight: 500 !important;
        transition: all 0.2s !important;
        box-shadow: var(--shadow) !important;
        font-family: inherit !important;
    }
    .stButton > button:hover {
        background: var(--primary-hover) !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3) !important;
    }
    .stButton > button:active {
        transform: translateY(0) !important;
    }
    
    /* ===== COMMENTAIRES ===== */
    .comment-card {
        background: var(--surface);
        border-radius: var(--radius);
        padding: 1rem 1.2rem;
        margin: 0.8rem 0;
        border-left: 4px solid var(--success);
        transition: box-shadow 0.2s;
    }
    .comment-card:hover {
        box-shadow: var(--shadow);
    }
    .comment-card.negatif { border-left-color: #ef4444; }
    .comment-card.neutre { border-left-color: #f59e0b; }
    .comment-card.mixed { border-left-color: #8b5cf6; }
    
    .comment-header {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-bottom: 0.5rem;
        flex-wrap: wrap;
    }
    .sentiment-badge {
        padding: 0.2rem 0.6rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
    }
    .sentiment-badge.positif {
        background: rgba(34, 197, 94, 0.15);
        color: var(--success);
    }
    .sentiment-badge.negatif {
        background: rgba(239, 68, 68, 0.15);
        color: #ef4444;
    }
    .sentiment-badge.neutre {
        background: rgba(245, 158, 11, 0.15);
        color: #f59e0b;
    }
    
    /* ===== FILTRES ===== */
    .filter-section {
        background: var(--surface);
        padding: 1rem;
        border-radius: var(--radius);
        margin-bottom: 1rem;
        border: 1px solid var(--border);
    }
    
    /* ===== GRAPHIQUES ===== */
    .chart-container {
        background: var(--background);
        border-radius: var(--radius);
        padding: 1rem;
        border: 1px solid var(--border);
        margin: 1rem 0;
    }
    
    /* ===== EXPANDERS ===== */
    .streamlit-expanderHeader {
        background: var(--surface) !important;
        border-radius: 8px !important;
        border: 1px solid var(--border) !important;
        font-weight: 500 !important;
    }
    .streamlit-expanderContent {
        background: var(--background) !important;
        border: 1px solid var(--border) !important;
        border-radius: 0 0 8px 8px !important;
        padding: 1rem !important;
    }
    
    /* ===== TABLEAUX ===== */
    .dataframe {
        border-radius: var(--radius) !important;
        overflow: hidden !important;
        border: 1px solid var(--border) !important;
        font-size: 0.9rem !important;
    }
    .dataframe thead {
        background: var(--surface) !important;
    }
    
    /* ===== ALERTES & INFO ===== */
    .stInfo, .stSuccess, .stWarning, .stError {
        border-radius: 8px !important;
        border-left: 4px solid var(--primary) !important;
    }
    
    /* ===== UTILITIES ===== */
    .text-center { text-align: center; }
    .mb-1 { margin-bottom: 0.5rem; }
    .mb-2 { margin-bottom: 1rem; }
    .mt-1 { margin-top: 0.5rem; }
    .mt-2 { margin-top: 1rem; }
    .flex { display: flex; }
    .items-center { align-items: center; }
    .gap-2 { gap: 0.5rem; }
    .justify-between { justify-content: space-between; }
    
    /* ===== ANIMATIONS ===== */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .fade-in {
        animation: fadeIn 0.3s ease-out;
    }
    
    /* ===== SCROLLBAR ===== */
    ::-webkit-scrollbar {
        width: 6px;
        height: 6px;
    }
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    ::-webkit-scrollbar-thumb {
        background: var(--border);
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: var(--text-secondary);
    }
    
    /* ===== TEXTAREA & INPUT ===== */
    input, textarea, select {
        border-radius: 8px !important;
        border: 1px solid var(--border) !important;
    }
    input:focus, textarea:focus, select:focus {
        border-color: var(--primary) !important;
        box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1) !important;
    }
    </style>
    
    <!-- Google Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    """, unsafe_allow_html=True)


# ============================================
# COMPOSANTS UI
# ============================================

def kpi_card(label: str, value: str, delta: str = None, 
             delta_type: str = None, icon: str = "📊", 
             color: str = "#2563eb"):
    """
    Crée une carte KPI stylisée
    
    Args:
        label: Titre de la métrique
        value: Valeur principale à afficher
        delta: Variation (optionnel)
        delta_type: 'positive', 'negative' ou None
        icon: Emoji ou icône
        color: Couleur de la bordure gauche
    """
    delta_class = ""
    delta_symbol = ""
    if delta:
        if delta_type == "positive":
            delta_class = "positive"
            delta_symbol = "▲"
        elif delta_type == "negative":
            delta_class = "negative"
            delta_symbol = "▼"
        else:
            delta_symbol = "•"
    
    delta_html = f'<div class="kpi-delta {delta_class}">{delta_symbol} {delta}</div>' if delta else ""
    
    st.markdown(f"""
    <div class="kpi-card" style="border-left-color: {color};">
        <div class="kpi-label">{icon} {label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def comment_card(text: str, sentiment: str, date: str = None, 
                 source: str = None, max_length: int = 300):
    """
    Affiche un commentaire avec styling adapté au sentiment
    
    Args:
        text: Contenu du commentaire
        sentiment: 'positif', 'negatif', 'neutre' ou 'mixed'
        date: Date du commentaire (optionnel)
        source: Source du commentaire (optionnel)
        max_length: Longueur max avant troncature
    """
    emojis = {"positif": "😊", "negatif": "😠", "neutre": "😐", "mixed": "🔄"}
    emoji = emojis.get(sentiment, "📝")
    
    # Tronquer le texte si nécessaire
    if len(str(text)) > max_length:
        text = str(text)[:max_length] + "..."
    
    # Méta-information
    meta_parts = []
    if date:
        meta_parts.append(f"📅 {date}")
    if source:
        meta_parts.append(f"📱 {source}")
    meta_html = f'<div style="color:var(--text-secondary); font-size:0.8rem; margin-top:0.5rem;">{" | ".join(meta_parts)}</div>' if meta_parts else ""
    
    st.markdown(f"""
    <div class="comment-card {sentiment} fade-in">
        <div class="comment-header">
            <span class="sentiment-badge {sentiment}">{emoji} {sentiment.upper()}</span>
        </div>
        <div style="line-height:1.5; color:var(--text-primary);">{text}</div>
        {meta_html}
    </div>
    """, unsafe_allow_html=True)


def section_title(title: str, subtitle: str = None, icon: str = "📌", 
                  show_border: bool = True):
    """
    Affiche un titre de section harmonisé
    
    Args:
        title: Titre principal
        subtitle: Sous-titre optionnel
        icon: Emoji ou icône
        show_border: Afficher une bordure inférieure
    """
    border_style = "border-bottom:2px solid var(--primary); padding-bottom:0.5rem;" if show_border else ""
    html = f'<h2 style="color:var(--text-primary); {border_style} margin:1.5rem 0 0.5rem;">{icon} {title}</h2>'
    
    if subtitle:
        html += f'<p style="color:var(--text-secondary); margin:-0.3rem 0 1rem; font-size:0.95rem;">{subtitle}</p>'
    
    st.markdown(html, unsafe_allow_html=True)


def info_box(message: str, type: str = "info", icon: str = None):
    """
    Affiche une boîte d'information stylisée
    
    Args:
        message: Contenu du message
        type: 'info', 'success', 'warning', 'error'
        icon: Emoji personnalisé (optionnel)
    """
    icons = {
        "info": "ℹ️",
        "success": "✅", 
        "warning": "⚠️",
        "error": "❌"
    }
    colors = {
        "info": "#2563eb",
        "success": "#22c55e",
        "warning": "#f59e0b",
        "error": "#ef4444"
    }
    
    icon_display = icon or icons.get(type, "ℹ️")
    color = colors.get(type, "#2563eb")
    
    st.markdown(f"""
    <div style="
        background: var(--surface);
        border-left: 4px solid {color};
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        display: flex;
        align-items: flex-start;
        gap: 0.5rem;
    ">
        <span style="font-size:1.2rem;">{icon_display}</span>
        <span style="color:var(--text-primary); line-height:1.4;">{message}</span>
    </div>
    """, unsafe_allow_html=True)


def loading_spinner(text: str = "Chargement..."):
    """Affiche un indicateur de chargement personnalisé"""
    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:0.5rem; color:var(--text-secondary); padding:1rem;">
        <div style="
            width:20px; height:20px;
            border:2px solid var(--border);
            border-top-color: var(--primary);
            border-radius:50%;
            animation: spin 1s linear infinite;
        "></div>
        <span>{text}</span>
    </div>
    <style>
    @keyframes spin {{
        to {{ transform: rotate(360deg); }}
    }}
    </style>
    """, unsafe_allow_html=True)


def empty_state(message: str, icon: str = "🔍", action_text: str = None, action_callback=None):
    """
    Affiche un état vide avec message et option d'action
    
    Args:
        message: Message à afficher
        icon: Emoji pour l'illustration
        action_text: Texte du bouton d'action (optionnel)
        action_callback: Fonction à appeler si bouton cliqué
    """
    st.markdown(f"""
    <div style="
        text-align:center; 
        padding:3rem 2rem; 
        color:var(--text-secondary);
        background:var(--surface);
        border-radius:var(--radius);
        border:1px dashed var(--border);
    ">
        <div style="font-size:3rem; margin-bottom:1rem;">{icon}</div>
        <p style="font-size:1.1rem; color:var(--text-primary); margin:0 0 1rem;">{message}</p>
    </div>
    """, unsafe_allow_html=True)
    
    if action_text and action_callback:
        col = st.columns([1, 2, 1])[1]
        with col:
            if st.button(action_text, use_container_width=True):
                action_callback()


# ============================================
# CONFIGURATION PLOTLY
# ============================================
PLOTLY_CONFIG = {
    'displayModeBar': True,
    'displaylogo': False,
    'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
    'responsive': True
}

COLOR_MAP = {
    'positif': '#22c55e',    # Vert
    'negatif': '#ef4444',    # Rouge
    'neutre': '#f59e0b',     # Orange
    'mixed': '#8b5cf6',      # Violet
}

PLOTLY_LAYOUT_DEFAULTS = {
    'plot_bgcolor': 'white',
    'paper_bgcolor': 'white',
    'font': dict(family='Inter, sans-serif', color='#1e293b', size=12),
    'title_font': dict(family='Inter, sans-serif', size=16, color='#1e293b'),
    'legend': dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    'margin': dict(l=40, r=40, t=60, b=40),
    'hovermode': 'x unified'
}


def apply_plotly_theme(fig):
    """Applique le thème personnalisé à un graphique Plotly"""
    fig.update_layout(**PLOTLY_LAYOUT_DEFAULTS)
    fig.update_traces(marker_line_width=0)
    return fig