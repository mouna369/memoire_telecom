import streamlit as st

def load_fontawesome():
    st.markdown("""
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    """, unsafe_allow_html=True)

MAIN_CSS = """
<style>
    /* Supprimer la navigation par défaut de Streamlit */
    [data-testid="stSidebarNav"] {
        display: none !important;
    }
    
    /* Sidebar : fond gris clair / bleuté */
    [data-testid="stSidebar"] {
        background: #eef2f6 !important;
        border-right: 1px solid #cbd5e1;
        padding-top: 1.5rem;
    }
    
    /* Tous les textes dans la sidebar : foncés */
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] .stCaption,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] span {
        color: #1e293b !important;
    }
    
    /* Liens de page (st.page_link) : foncés */
    [data-testid="stSidebar"] a {
        color: #1e293b !important;
        text-decoration: none !important;
        font-weight: 500;
    }
    [data-testid="stSidebar"] a:hover {
        color: #0f3b5c !important;
        background: rgba(0,0,0,0.05);
        border-radius: 8px;
    }
    
    /* Bouton Log out */
    [data-testid="stSidebar"] .stButton button {
        color: #1e293b !important;
        background: transparent !important;
        border: none !important;
        text-align: left !important;
        font-weight: 500;
    }
    [data-testid="stSidebar"] .stButton button:hover {
        color: #0f3b5c !important;
        background: rgba(0,0,0,0.05) !important;
        border-radius: 8px;
    }
    
    /* Profil et logo dans la sidebar */
    .sidebar-profile i {
        color: #2c3e50 !important;
        background: #d1d9e6 !important;
    }
    .sidebar-profile h4 {
        color: #0f172a !important;
    }
    .sidebar-profile p {
        color: #475569 !important;
    }
    .sidebar-logo h3 {
        color: #0f3b5c !important;
    }
    .menu-section-title {
        color: #475569 !important;
    }
    
    /* Icônes du menu */
    [data-testid="stSidebar"] i {
        color: #2c3e50 !important;
    }
    
    /* Corps principal */
    body {
        background-color: #f8fafc;
        font-family: 'Inter', 'Segoe UI', sans-serif;
    }
    
    /* Logo et profil (styles généraux) */
    .sidebar-logo {
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .sidebar-logo i {
        font-size: 3rem;
        color: #2dd4bf;
    }
    .sidebar-profile {
        text-align: center;
        margin-bottom: 2rem;
        padding: 0 1rem;
    }
    .sidebar-profile i {
        font-size: 3.5rem;
        border-radius: 50%;
        padding: 0.5rem;
    }
    
    /* Menu items (si utilisés) */
    .menu-item {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 10px 16px;
        margin: 4px 12px;
        border-radius: 12px;
        color: #1e293b;
        font-weight: 500;
        transition: all 0.2s;
        cursor: pointer;
    }
    .menu-item i {
        width: 24px;
        font-size: 1.2rem;
    }
    .menu-item:hover {
        background: rgba(0,0,0,0.05);
        color: #0f3b5c;
    }
    .menu-item.active {
        background: rgba(45, 212, 191, 0.2);
        color: #2dd4bf;
        border-left: 3px solid #2dd4bf;
    }
    
    /* Cartes KPI / Earnings */
    .earnings-card {
        background: white;
        border-radius: 24px;
        padding: 1rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        border: 1px solid #e2e8f0;
        margin-bottom: 1.5rem;
    }
    .section-title {
        font-size: 1.3rem;
        font-weight: 600;
        color: #0f3b5c;
        margin: 1rem 0 1rem 0;
        border-left: 4px solid #2dd4bf;
        padding-left: 12px;
    }
    
    /* Messages */
    .message-item {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 12px 0;
        border-bottom: 1px solid #eef2f6;
    }
    .message-avatar {
        width: 40px;
        height: 40px;
        background: #e2e8f0;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #0f3b5c;
    }
    .message-content {
        flex: 1;
    }
    .message-name {
        font-weight: 600;
        color: #1e293b;
    }
    .message-text {
        font-size: 0.8rem;
        color: #64748b;
    }
    
    /* Upgrade card */
    .upgrade-card {
        background: linear-gradient(135deg, #0f3b5c 0%, #1e6b5e 100%);
        border-radius: 20px;
        padding: 1.2rem;
        color: white;
        text-align: center;
        margin: 1rem 0;
    }
    .upgrade-card button {
        background: #2dd4bf;
        border: none;
        color: #0f3b5c;
        font-weight: bold;
        border-radius: 40px;
        padding: 0.4rem 1.2rem;
        margin-top: 0.5rem;
        cursor: pointer;
    }
    
    /* Projets & Progression */
    .project-item, .progress-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .project-label {
        font-weight: 500;
        color: #334155;
    }
    .project-percent {
        font-weight: 700;
        color: #0f3b5c;
    }
    .progress-detail {
        font-size: 0.75rem;
        color: #64748b;
    }
    
    /* Calendrier */
    .calendar {
        background: white;
        border-radius: 20px;
        padding: 1rem;
        border: 1px solid #e2e8f0;
    }
    .calendar-grid {
        display: grid;
        grid-template-columns: repeat(7, 1fr);
        gap: 8px;
        text-align: center;
        margin-top: 1rem;
    }
    .calendar-day-header {
        font-weight: 600;
        color: #475569;
        font-size: 0.8rem;
    }
    .calendar-day {
        padding: 8px 0;
        border-radius: 40px;
        font-size: 0.85rem;
        color: #1e293b;
    }
    .calendar-day.other-month {
        color: #cbd5e1;
    }
    .calendar-day.today {
        background: #2dd4bf;
        color: white;
        font-weight: bold;
    }
    
    hr {
        margin: 1rem 0;
    }
    .footer {
        text-align: center;
        font-size: 0.7rem;
        color: #94a3b8;
        margin-top: 2rem;
    }
</style>
"""