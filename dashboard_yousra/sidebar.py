# sidebar.py - Version compatible avec votre Streamlit
import streamlit as st
from style import load_fontawesome, MAIN_CSS
import os
from PIL import Image
import io

def optimize_logo(logo_path, max_width=200):
    """Optimise le logo pour un affichage net"""
    try:
        img = Image.open(logo_path)
        
        # Calculer les nouvelles dimensions
        ratio = max_width / img.width
        new_width = max_width
        new_height = int(img.height * ratio)
        
        # Redimensionner avec un bon algorithme
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # Sauvegarder dans un buffer
        buf = io.BytesIO()
        img.save(buf, format='PNG', optimize=True)
        buf.seek(0)
        return buf
    except Exception as e:
        return None

def render_sidebar():
    # CSS personnalisé pour la sidebar
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
        .logo-container {
            text-align: center;
            padding: 20px 0 15px 0;
            border-bottom: 1px solid #334155;
            margin-bottom: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        load_fontawesome()
        st.markdown(MAIN_CSS, unsafe_allow_html=True)

        # ============================================
        # LOGO - Version corrigée (pas de use_container_width)
        # ============================================
        
        # Chercher le logo
        logo_paths = [
            "/home/mouna/projet_telecom/dashboard_yousra/assets/Logo_Algérie_Télécom.png",
            "/home/mouna/projet_telecom/dashboard/assets/Logo_Algérie_Télécom.svg.png",
            "assets/Logo_Algérie_Télécom.png",
        ]
        
        logo_found = None
        for path in logo_paths:
            if os.path.exists(path):
                logo_found = path
                break
        
        if logo_found:
            # Version avec largeur fixe (compatible)
            optimized_logo = optimize_logo(logo_found, max_width=200)
            if optimized_logo:
                st.image(optimized_logo, width=240)  # ← CHANGÉ : width au lieu de use_container_width
            else:
                st.image(logo_found, width=200)  # ← CHANGÉ
        else:
            # Fallback textuel
            st.markdown(
                """
                <div class="logo-container">
                    <i class="fas fa-tower-cell" style="font-size:3rem; color:#38bdf8;"></i>
                    <h3 style="color:white; margin:10px 0 5px 0;">Algérie Télécom</h3>
                    <p style="color:#94a3b8; font-size:0.7rem;">Customer Experience</p>
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

        # Pages
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