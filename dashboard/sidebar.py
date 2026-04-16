# sidebar.py
import streamlit as st
from style import load_fontawesome, MAIN_CSS
import os

def render_sidebar():
    # CSS personnalisé pour la sidebar (fond, couleurs, etc.)
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] {
            background-color: #0f172a;
        }
        [data-testid="stSidebar"] .stMarkdown, 
        [data-testid="stSidebar"] .stCaption {
            color: #e2e8f0;
        }
        .menu-section-title {
            color: #94a3b8;
            font-size: 0.75rem;
            letter-spacing: 1px;
            margin: 20px 0 10px 0;
        }
        .sidebar-profile {
            text-align: center;
            padding: 15px 0;
            border-bottom: 1px solid #334155;
            margin-bottom: 20px;
        }
        .sidebar-profile i {
            font-size: 2.5rem;
            color: #38bdf8;
        }
        .sidebar-profile h4 {
            margin: 5px 0 0 0;
            color: white;
        }
        .sidebar-profile p {
            font-size: 0.7rem;
            color: #94a3b8;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        load_fontawesome()
        st.markdown(MAIN_CSS, unsafe_allow_html=True)

        # Logo
        logo_path = "assets/logo_algérie_télécom.svg.png"
        if os.path.exists(logo_path):
            st.image(logo_path, width=80)
        else:
            st.markdown(
                """
                <div style="text-align:center; margin-bottom:20px;">
                    <i class="fas fa-tower-cell" style="font-size:2rem; color:#38bdf8;"></i>
                    <h3 style="color:white;">Algérie Télécom</h3>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Profil (statique)
        st.markdown(
            """
            <div class="sidebar-profile">
                <i class="fas fa-user-circle"></i>
                <h4>Barly Vallendito</h4>
                <p>Premium Account</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="menu-section-title">MAIN MENU</div>', unsafe_allow_html=True)

        # Pages : (label, icône, chemin)
        pages = {
            "Dashboard": ("fas fa-tachometer-alt", "app.py"),
            "Analytics": ("fas fa-chart-line", "pages/2_Analyse.py"),
            "Commentaire": ("fas fa-comment", "pages/3_Commentaires.py"),
            "Statistiques": ("fas fa-chart-simple", "pages/4_Statistiques.py"),
            "Chatbot": ("fas fa-robot", "pages/chatbot.py"),
        }

        for label, (icon, path) in pages.items():
            col_icon, col_link = st.columns([0.2, 0.8])
            with col_icon:
                st.markdown(f'<i class="{icon}" style="color: #cbd5e1; font-size: 1.2rem;"></i>', unsafe_allow_html=True)
            with col_link:
                if os.path.exists(path):
                    st.page_link(path, label=label, use_container_width=True)
                else:
                    st.markdown(f'<span style="color: #64748b; font-size: 0.9rem;">{label} (fichier manquant)</span>', unsafe_allow_html=True)

        st.markdown("---")

        # Logout
        col_icon, col_link = st.columns([0.2, 0.8])
        with col_icon:
            st.markdown('<i class="fas fa-sign-out-alt" style="color: #cbd5e1; font-size: 1.2rem;"></i>', unsafe_allow_html=True)
        with col_link:
            if st.button("Log out", use_container_width=True, key="logout_btn"):
                st.session_state.clear()
                st.rerun()

        st.caption("© 2026 Algérie Télécom")