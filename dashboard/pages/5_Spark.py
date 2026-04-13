# # # pages/5_Spark.py
# # import streamlit as st
# # import os

# # st.set_page_config(
# #     page_title="Spark Dashboard",
# #     page_icon="⚡",
# #     layout="wide"
# # )

# # st.title("⚡ Dashboard Spark - Traitement distribué")

# # st.markdown("""
# # Ce dashboard affiche en temps réel l'état du traitement Spark :
# # - **Workers actifs** : état et charge des 3 workers
# # - **Pipeline distribué** : progression étape par étape
# # - **Benchmark** : comparaison 1 worker vs 3 workers
# # - **Métriques temps réel** : documents traités, débit, temps écoulé
# # """)

# # st.markdown("---")

# # # Intégrer le dashboard HTML dans un iframe
# # # Le dashboard Spark tourne sur le port 5055
# # spark_dashboard_url = "http://localhost:5055"

# # st.info("⚡ Dashboard Spark - Mise à jour en temps réel (1 seconde)")

# # # Iframe pour intégrer le dashboard
# # st.components.v1.html(
# #     f"""
# #     <iframe src="{spark_dashboard_url}" 
# #             style="width:100%; height:800px; border:none; border-radius:8px;"
# #             title="Spark Dashboard">
# #     </iframe>
# #     """,
# #     height=850,
# #     scrolling=True
# # )

# # st.caption("📊 Données mises à jour automatiquement | Spark 4.1.1 | MongoDB")




# # pages/5_Spark.py
# import streamlit as st

# st.set_page_config(
#     page_title="Spark Dashboard",
#     page_icon="⚡",
#     layout="wide"
# )

# # CSS pour que l'iframe prenne tout l'espace disponible
# st.markdown("""
# <style>
#     /* Supprimer les marges du bloc principal */
#     .main .block-container {
#         padding-top: 0rem;
#         padding-bottom: 0rem;
#         padding-left: 0rem;
#         padding-right: 0rem;
#         # max-width: 100%;
#     }
    
#     /* Supprimer l'espace en haut */
#     header {
#         display: none;
#     }
    
#     /* L'iframe prend tout l'espace */
#     .stMarkdown {
#         margin: 0;
#         padding: 0;
#     }
    
#     iframe {
#         width: 100%;
#         height: calc(100vh - 50px);
#         border: none;
#     }
# </style>
# """, unsafe_allow_html=True)

# st.markdown("")  # Petit hack pour déclencher le CSS

# # Iframe plein écran (garde le menu)
# spark_dashboard_url = "http://localhost:5055"

# st.components.v1.html(
#     f"""
#     <iframe src="{spark_dashboard_url}" 
#             style="width:100%; height:calc(100vh - 80px); border:none;"
#             title="Spark Dashboard">
#     </iframe>
#     """,
#     height=1000,
#     scrolling=True
# )
