# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from sidebar import render_sidebar
from style import load_fontawesome, MAIN_CSS

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Algérie Télécom",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Charger sidebar et styles
render_sidebar()

# ========== CONTENU PRINCIPAL ==========
st.markdown('<h1 style="color: #0f3b5c;">Dashboard</h1>', unsafe_allow_html=True)

# --- Earnings Chart (bar chart) ---
st.markdown('<div class="section-title"><i class="fas fa-chart-simple"></i> Earnings</div>', unsafe_allow_html=True)

earnings_data = pd.DataFrame({
    "Month": ["Jan", "Feb", "Mar", "Apr", "May"],
    "Earnings": [1200, 1900, 3100, 4500, 5200]
})
fig = px.bar(earnings_data, x="Month", y="Earnings", 
             color_discrete_sequence=["#2dd4bf"],
             labels={"Earnings": "$", "Month": ""},
             text_auto=True)
fig.update_layout(plot_bgcolor="white", height=400, margin=dict(l=0, r=0, t=0, b=0))
fig.update_traces(textposition="outside", marker_line_color="#0f3b5c", marker_line_width=1)
st.plotly_chart(fig, use_container_width=True)

# Deux colonnes : Messages et Projets / Upgrade
col1, col2 = st.columns([1, 1])

with col1:
    st.markdown('<div class="section-title"><i class="fas fa-envelope"></i> Message</div>', unsafe_allow_html=True)
    messages = [
        ("Crystal Maiden", "I need illustration for our new project!"),
        ("Klaus Baudelaires", "Hello excel i have a great Project..."),
        ("Andreas raquel", "Let collaborate for our website project."),
        ("Alison Parker", "Sorry maybe i cant finish the work."),
        ("Lemony snicket", "I will send the background illustration.")
    ]
    for name, text in messages:
        st.markdown(f"""
        <div class="message-item">
            <div class="message-avatar"><i class="fas fa-user"></i></div>
            <div class="message-content">
                <div class="message-name">{name}</div>
                <div class="message-text">{text}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Upgrade card
    st.markdown("""
    <div class="upgrade-card">
        <i class="fas fa-crown" style="font-size: 2rem;"></i>
        <h4>Upgrade your member and get the benefit</h4>
        <button>Become pro!</button>
    </div>
    """, unsafe_allow_html=True)

with col2:
    # Project section
    st.markdown('<div class="section-title"><i class="fas fa-diagram-project"></i> Project</div>', unsafe_allow_html=True)
    projects = {
        "UI/UX": 46,
        "APP": 32,
        "Desktop": 78,
        "Logo": 15
    }
    for name, percent in projects.items():
        st.markdown(f"""
        <div class="project-item">
            <span class="project-label">{name}</span>
            <span class="project-percent">{percent}%</span>
        </div>
        """, unsafe_allow_html=True)
        st.progress(percent / 100, text="")
    
    st.markdown("---")
    # Progress section
    st.markdown('<div class="section-title"><i class="fas fa-chart-progress"></i> Progress</div>', unsafe_allow_html=True)
    progresses = [
        ("Illustration", 4, 10),
        ("Desktop", 4, 10),
        ("Mobile UX", 4, 10),
        ("Logo", 4, 10)
    ]
    for label, done, total in progresses:
        col_a, col_b = st.columns([3, 1])
        with col_a:
            st.markdown(f"**{label}**<br><span class='progress-detail'>{done} of {total} completed</span>", unsafe_allow_html=True)
            st.progress(done/total)
        with col_b:
            st.markdown(f"<div style='text-align: right;'>{done}/{total}</div>", unsafe_allow_html=True)

# --- Calendar (April 2021) ---
st.markdown('<div class="section-title"><i class="fas fa-calendar"></i> April 2021</div>', unsafe_allow_html=True)

# Générer le calendrier d'avril 2021
import calendar
cal = calendar.monthcalendar(2021, 4)
days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
html_cal = '<div class="calendar"><div class="calendar-grid">'
for d in days:
    html_cal += f'<div class="calendar-day-header">{d}</div>'
for week in cal:
    for day in week:
        if day == 0:
            html_cal += '<div class="calendar-day other-month"></div>'
        else:
            today_class = "today" if (day == 14) else ""  # exemple: 14 avril mis en avant
            html_cal += f'<div class="calendar-day {today_class}">{day}</div>'
html_cal += '</div></div>'
st.markdown(html_cal, unsafe_allow_html=True)

st.markdown('<div class="footer"><i class="fas fa-chart-line"></i> Données en temps réel | <i class="fas fa-shield-alt"></i> Sécurisé | Algérie Télécom © 2026</div>', unsafe_allow_html=True)