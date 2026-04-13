# # # # app.py
# # # import streamlit as st

# # # st.set_page_config(
# # #     page_title="Dashboard Télécom Algérie",
# # #     page_icon="📊",
# # #     layout="wide"
# # # )

# # # st.title("🏠 Tableau de bord - Télécom Algérie")
# # # st.markdown("---")

# # # st.markdown("""
# # # ## 📊 Bienvenue sur votre dashboard d'analyse des sentiments

# # # Ce dashboard vous permet d'analyser les commentaires clients des opérateurs télécoms algériens.

# # # ### 📈 Pages disponibles

# # # | Page | Description |
# # # |------|-------------|
# # # | **Overview** | Vue d'ensemble avec KPIs et graphiques principaux |
# # # | **Analyse** | Analyse approfondie des données (évolutions, distributions) |
# # # | **Commentaires** | Exploration détaillée des commentaires clients |
# # # | **Statistiques** | Analyses statistiques avancées (tests, corrélations) |
# # # | **🤖 ChatBot IA** | Conversation intelligente + analyse de vos données |

# # # ### 🚀 Fonctionnalités

# # # - ✅ Analyse des sentiments (positif/négatif)
# # # - ✅ Visualisations interactives
# # # - ✅ Filtres dynamiques
# # # - ✅ Export des données (CSV)
# # # - ✅ Mise à jour en temps réel
# # # - ✅ ChatBot IA (Ollama · Mistral 7B)

# # # ### 📊 Sources de données

# # # - Commentaires Facebook
# # # - Données annotées manuellement
# # # - Base MongoDB : `telecom_algerie`

# # # ---
# # # *Dernière mise à jour : Avril 2026*
# # # """)

# # # st.sidebar.success("✅ Dashboard prêt !")
# # # st.sidebar.info("Utilisez le menu à gauche pour naviguer entre les pages")



# # # app.py - Fichier principal d'authentification
# # import dash
# # from dash import dcc, html, Input, Output, State, callback_context
# # import dash_bootstrap_components as dbc
# # import hashlib
# # import re
# # import json
# # import os
# # from datetime import datetime

# # # ============================================
# # # CONFIGURATION
# # # ============================================

# # app = dash.Dash(
# #     __name__,
# #     external_stylesheets=[dbc.themes.BOOTSTRAP],
# #     suppress_callback_exceptions=True,
# #     title="Télécom DZ - Authentification"
# # )

# # server = app.server

# # # ============================================
# # # GESTION DES UTILISATEURS
# # # ============================================

# # USERS_FILE = "users.json"

# # def hash_password(password):
# #     return hashlib.sha256(password.encode()).hexdigest()

# # def load_users():
# #     if os.path.exists(USERS_FILE):
# #         with open(USERS_FILE, 'r') as f:
# #             return json.load(f)
# #     return {}

# # def save_users(users):
# #     with open(USERS_FILE, 'w') as f:
# #         json.dump(users, f, indent=2)

# # def validate_email(email):
# #     pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
# #     return re.match(pattern, email) is not None

# # def validate_password(password):
# #     score = 0
# #     if len(password) >= 8:
# #         score += 1
# #     if re.search(r'[A-Z]', password):
# #         score += 1
# #     if re.search(r'[a-z]', password):
# #         score += 1
# #     if re.search(r'[0-9]', password):
# #         score += 1
    
# #     if score >= 3:
# #         return True, "Fort"
# #     elif score >= 2:
# #         return False, "Moyen"
# #     else:
# #         return False, "Faible"

# # # ============================================
# # # STYLES CSS (Bleu, Vert, Blanc)
# # # ============================================

# # app.index_string = '''
# # <!DOCTYPE html>
# # <html>
# #     <head>
# #         {%metas%}
# #         <title>{%title%}</title>
# #         {%favicon%}
# #         {%css%}
# #         <style>
# #             @import url('https://fonts.googleapis.com/css2?family=Inter:wght@100..900&display=swap');
            
# #             * {
# #                 margin: 0;
# #                 padding: 0;
# #                 box-sizing: border-box;
# #             }
            
# #             body {
# #                 font-family: 'Inter', sans-serif;
# #                 background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
# #                 min-height: 100vh;
# #                 display: flex;
# #                 align-items: center;
# #                 justify-content: center;
# #             }
            
# #             .auth-container {
# #                 max-width: 500px;
# #                 width: 90%;
# #                 margin: 2rem auto;
# #             }
            
# #             .auth-card {
# #                 background: white;
# #                 border-radius: 32px;
# #                 padding: 2.5rem;
# #                 box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
# #                 animation: fadeIn 0.5s ease-out;
# #             }
            
# #             @keyframes fadeIn {
# #                 from {
# #                     opacity: 0;
# #                     transform: translateY(20px);
# #                 }
# #                 to {
# #                     opacity: 1;
# #                     transform: translateY(0);
# #                 }
# #             }
            
# #             .logo-section {
# #                 text-align: center;
# #                 margin-bottom: 2rem;
# #             }
            
# #             .logo-icon {
# #                 font-size: 3.5rem;
# #                 animation: pulse 2s infinite;
# #             }
            
# #             @keyframes pulse {
# #                 0%, 100% { transform: scale(1); }
# #                 50% { transform: scale(1.05); }
# #             }
            
# #             .logo-title {
# #                 font-size: 2rem;
# #                 font-weight: 800;
# #                 background: linear-gradient(135deg, #3B82F6, #10B981);
# #                 -webkit-background-clip: text;
# #                 -webkit-text-fill-color: transparent;
# #                 margin-top: 0.5rem;
# #             }
            
# #             .logo-subtitle {
# #                 color: #64748B;
# #                 font-size: 0.875rem;
# #                 margin-top: 0.5rem;
# #             }
            
# #             .tabs-container {
# #                 display: flex;
# #                 gap: 0.5rem;
# #                 background: #F1F5F9;
# #                 padding: 0.5rem;
# #                 border-radius: 16px;
# #                 margin-bottom: 2rem;
# #             }
            
# #             .tab-btn {
# #                 flex: 1;
# #                 padding: 0.75rem;
# #                 border: none;
# #                 background: transparent;
# #                 font-weight: 600;
# #                 font-size: 0.875rem;
# #                 cursor: pointer;
# #                 border-radius: 12px;
# #                 transition: all 0.3s ease;
# #                 color: #64748B;
# #             }
            
# #             .tab-btn.active {
# #                 background: linear-gradient(135deg, #3B82F6, #10B981);
# #                 color: white;
# #                 box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
# #             }
            
# #             .form-group {
# #                 margin-bottom: 1.5rem;
# #             }
            
# #             .form-label {
# #                 display: block;
# #                 margin-bottom: 0.5rem;
# #                 color: #334155;
# #                 font-weight: 600;
# #                 font-size: 0.875rem;
# #             }
            
# #             .form-control {
# #                 width: 100%;
# #                 padding: 0.875rem 1rem;
# #                 border: 2px solid #E2E8F0;
# #                 border-radius: 14px;
# #                 font-size: 0.95rem;
# #                 transition: all 0.3s ease;
# #                 background: #F8FAFC;
# #             }
            
# #             .form-control:focus {
# #                 outline: none;
# #                 border-color: #3B82F6;
# #                 box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
# #                 background: white;
# #             }
            
# #             .btn-primary {
# #                 width: 100%;
# #                 padding: 1rem;
# #                 background: linear-gradient(135deg, #3B82F6, #10B981);
# #                 color: white;
# #                 border: none;
# #                 border-radius: 14px;
# #                 font-size: 1rem;
# #                 font-weight: 700;
# #                 cursor: pointer;
# #                 transition: all 0.3s ease;
# #             }
            
# #             .btn-primary:hover {
# #                 transform: translateY(-2px);
# #                 box-shadow: 0 10px 25px -5px rgba(59, 130, 246, 0.5);
# #             }
            
# #             .message-success {
# #                 background: #D1FAE5;
# #                 color: #065F46;
# #                 padding: 0.875rem;
# #                 border-radius: 12px;
# #                 margin-bottom: 1rem;
# #                 border-left: 4px solid #10B981;
# #                 font-weight: 500;
# #                 animation: slideDown 0.3s ease-out;
# #             }
            
# #             .message-error {
# #                 background: #FEE2E2;
# #                 color: #991B1B;
# #                 padding: 0.875rem;
# #                 border-radius: 12px;
# #                 margin-bottom: 1rem;
# #                 border-left: 4px solid #EF4444;
# #                 font-weight: 500;
# #                 animation: slideDown 0.3s ease-out;
# #             }
            
# #             @keyframes slideDown {
# #                 from {
# #                     opacity: 0;
# #                     transform: translateY(-10px);
# #                 }
# #                 to {
# #                     opacity: 1;
# #                     transform: translateY(0);
# #                 }
# #             }
            
# #             .divider {
# #                 text-align: center;
# #                 margin: 2rem 0;
# #                 position: relative;
# #             }
            
# #             .divider::before {
# #                 content: "";
# #                 position: absolute;
# #                 left: 0;
# #                 top: 50%;
# #                 width: 100%;
# #                 height: 1px;
# #                 background: linear-gradient(90deg, transparent, #E2E8F0, transparent);
# #             }
            
# #             .divider span {
# #                 background: white;
# #                 padding: 0 1rem;
# #                 position: relative;
# #                 color: #94A3B8;
# #                 font-size: 0.875rem;
# #             }
            
# #             .footer {
# #                 text-align: center;
# #                 margin-top: 2rem;
# #                 padding-top: 1.5rem;
# #                 border-top: 1px solid #E2E8F0;
# #                 color: #94A3B8;
# #                 font-size: 0.75rem;
# #             }
# #         </style>
# #     </head>
# #     <body>
# #         {%app_entry%}
# #         <footer>
# #             {%config%}
# #             {%scripts%}
# #             {%renderer%}
# #         </footer>
# #     </body>
# # </html>
# # '''

# # # ============================================
# # # LAYOUT
# # # ============================================

# # app.layout = html.Div([
# #     dcc.Store(id='auth-store', data={'authenticated': False, 'username': None}),
# #     dcc.Store(id='active-tab', data='login'),
# #     dcc.Location(id='url', refresh=False),
    
# #     html.Div([
# #         html.Div([
# #             html.Div([
# #                 # Logo
# #                 html.Div([
# #                     html.Div("📡", className="logo-icon"),
# #                     html.Div("Télécom DZ", className="logo-title"),
# #                     html.Div("Analyse intelligente des sentiments", className="logo-subtitle"),
# #                 ], className="logo-section"),
                
# #                 # Tabs
# #                 html.Div([
# #                     html.Button("🔐 CONNEXION", id="login-tab", className="tab-btn active", n_clicks=0),
# #                     html.Button("📝 INSCRIPTION", id="signup-tab", className="tab-btn", n_clicks=0),
# #                 ], className="tabs-container"),
                
# #                 # Message
# #                 html.Div(id="auth-message"),
                
# #                 # Formulaire Connexion
# #                 html.Div(id="login-form", children=[
# #                     html.Div([
# #                         html.Label("📧 Email", className="form-label"),
# #                         dcc.Input(id="login-email", type="email", className="form-control", placeholder="vous@entreprise.com"),
# #                     ], className="form-group"),
                    
# #                     html.Div([
# #                         html.Label("🔒 Mot de passe", className="form-label"),
# #                         dcc.Input(id="login-password", type="password", className="form-control", placeholder="Votre mot de passe"),
# #                     ], className="form-group"),
                    
# #                     html.Button("Se connecter", id="login-btn", className="btn-primary", n_clicks=0),
# #                 ]),
                
# #                 # Formulaire Inscription (caché par défaut)
# #                 html.Div(id="signup-form", style={"display": "none"}, children=[
# #                     html.Div([
# #                         html.Label("👤 Nom complet", className="form-label"),
# #                         dcc.Input(id="signup-name", type="text", className="form-control", placeholder="Jean Dupont"),
# #                     ], className="form-group"),
                    
# #                     html.Div([
# #                         html.Label("📧 Email", className="form-label"),
# #                         dcc.Input(id="signup-email", type="email", className="form-control", placeholder="jean@entreprise.com"),
# #                     ], className="form-group"),
                    
# #                     html.Div([
# #                         html.Label("🔒 Mot de passe", className="form-label"),
# #                         dcc.Input(id="signup-password", type="password", className="form-control", placeholder="8+ caractères, 1 majuscule, 1 chiffre"),
# #                     ], className="form-group"),
                    
# #                     html.Div([
# #                         html.Label("🔒 Confirmer mot de passe", className="form-label"),
# #                         dcc.Input(id="signup-confirm", type="password", className="form-control", placeholder="Retapez votre mot de passe"),
# #                     ], className="form-group"),
                    
# #                     html.Button("Créer mon compte", id="signup-btn", className="btn-primary", n_clicks=0),
# #                 ]),
                
# #                 # Divider
# #                 html.Div([
# #                     html.Div(html.Span("Ou continuer avec"), className="divider")
# #                 ]),
                
# #                 # Boutons sociaux
# #                 html.Div([
# #                     html.Button("🌐 Google", className="btn-primary", style={"background": "#DB4437", "margin-bottom": "0.5rem"}),
# #                     html.Button("💼 LinkedIn", className="btn-primary", style={"background": "#0077B5", "margin-bottom": "0.5rem"}),
# #                     html.Button("🐙 GitHub", className="btn-primary", style={"background": "#333"}),
# #                 ]),
                
# #                 # Footer
# #                 html.Div([
# #                     html.Div("© 2026 Télécom DZ - Analyse des sentiments"),
# #                     html.Div(style={"margin-top": "0.5rem"}, children=[
# #                         html.Span("🔵 Bleu · ", style={"color": "#3B82F6"}),
# #                         html.Span("🟢 Vert · ", style={"color": "#10B981"}),
# #                         html.Span("⚪ Blanc", style={"color": "#000000"}),
# #                     ]),
# #                 ], className="footer"),
                
# #             ], className="auth-card"),
# #         ], className="auth-container"),
# #     ], id="main-content"),
# # ])

# # # ============================================
# # # CALLBACKS
# # # ============================================

# # @app.callback(
# #     [Output("login-form", "style"),
# #      Output("signup-form", "style"),
# #      Output("login-tab", "className"),
# #      Output("signup-tab", "className"),
# #      Output("active-tab", "data")],
# #     [Input("login-tab", "n_clicks"),
# #      Input("signup-tab", "n_clicks")],
# #     prevent_initial_call=True
# # )
# # def switch_tabs(login_clicks, signup_clicks):
# #     ctx = callback_context
# #     if not ctx.triggered:
# #         return {}, {"display": "none"}, "tab-btn active", "tab-btn", "login"
    
# #     button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
# #     if button_id == "login-tab":
# #         return {}, {"display": "none"}, "tab-btn active", "tab-btn", "login"
# #     else:
# #         return {"display": "none"}, {}, "tab-btn", "tab-btn active", "signup"

# # @app.callback(
# #     [Output("auth-message", "children"),
# #      Output("auth-store", "data"),
# #      Output("url", "pathname")],
# #     [Input("login-btn", "n_clicks"),
# #      Input("signup-btn", "n_clicks")],
# #     [State("login-email", "value"),
# #      State("login-password", "value"),
# #      State("signup-name", "value"),
# #      State("signup-email", "value"),
# #      State("signup-password", "value"),
# #      State("signup-confirm", "value"),
# #      State("active-tab", "data"),
# #      State("auth-store", "data")],
# #     prevent_initial_call=True
# # )
# # def handle_auth(login_clicks, signup_clicks, login_email, login_password,
# #                 signup_name, signup_email, signup_password, signup_confirm,
# #                 active_tab, auth_data):
    
# #     ctx = callback_context
# #     if not ctx.triggered:
# #         return "", auth_data, "/"
    
# #     button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
# #     # LOGIN
# #     if button_id == "login-btn" and login_clicks > 0:
# #         if not login_email or not login_password:
# #             return html.Div("❌ Veuillez remplir tous les champs", className="message-error"), auth_data, "/"
        
# #         users = load_users()
# #         hashed = hash_password(login_password)
        
# #         if login_email in users and users[login_email]['password'] == hashed:
# #             auth_data['authenticated'] = True
# #             auth_data['username'] = users[login_email]['name']
# #             return html.Div(f"✅ Bienvenue {users[login_email]['name']} ! Redirection...", className="message-success"), auth_data, "/analyse"
# #         else:
# #             return html.Div("❌ Email ou mot de passe incorrect", className="message-error"), auth_data, "/"
    
# #     # SIGNUP
# #     elif button_id == "signup-btn" and signup_clicks > 0:
# #         if not signup_name or not signup_email or not signup_password:
# #             return html.Div("❌ Veuillez remplir tous les champs", className="message-error"), auth_data, "/"
        
# #         if not validate_email(signup_email):
# #             return html.Div("❌ Email invalide", className="message-error"), auth_data, "/"
        
# #         if signup_password != signup_confirm:
# #             return html.Div("❌ Les mots de passe ne correspondent pas", className="message-error"), auth_data, "/"
        
# #         is_valid, strength = validate_password(signup_password)
# #         if not is_valid:
# #             return html.Div(f"❌ Mot de passe trop faible (Force: {strength})", className="message-error"), auth_data, "/"
        
# #         users = load_users()
# #         if signup_email in users:
# #             return html.Div("❌ Cet email est déjà utilisé", className="message-error"), auth_data, "/"
        
# #         users[signup_email] = {
# #             'name': signup_name,
# #             'email': signup_email,
# #             'password': hash_password(signup_password),
# #             'created_at': datetime.now().isoformat()
# #         }
# #         save_users(users)
        
# #         return html.Div("✅ Compte créé avec succès ! Vous pouvez maintenant vous connecter", className="message-success"), auth_data, "/"
    
# #     return "", auth_data, "/"

# # # ============================================
# # # LANCEMENT
# # # ============================================

# # if __name__ == '__main__':
# #     app.run(debug=True, port=8050)

# # app.py - Page d'authentification avec redirection
# import dash
# from dash import dcc, html, Input, Output, State, callback_context
# import hashlib
# import re
# import json
# import os
# from datetime import datetime

# # ============================================
# # CONFIGURATION
# # ============================================

# app = dash.Dash(
#     __name__,
#     external_stylesheets=[],
#     suppress_callback_exceptions=True,
#     title="Télécom DZ - Authentification"
# )

# server = app.server

# # ============================================
# # GESTION DES UTILISATEURS
# # ============================================

# USERS_FILE = "users.json"

# def hash_password(password):
#     return hashlib.sha256(password.encode()).hexdigest()

# def load_users():
#     if os.path.exists(USERS_FILE):
#         with open(USERS_FILE, 'r') as f:
#             return json.load(f)
#     return {}

# def save_users(users):
#     with open(USERS_FILE, 'w') as f:
#         json.dump(users, f, indent=2)

# def validate_email(email):
#     pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
#     return re.match(pattern, email) is not None

# def validate_password(password):
#     score = 0
#     if len(password) >= 8:
#         score += 1
#     if re.search(r'[A-Z]', password):
#         score += 1
#     if re.search(r'[a-z]', password):
#         score += 1
#     if re.search(r'[0-9]', password):
#         score += 1
    
#     if score >= 3:
#         return True, "Fort"
#     elif score >= 2:
#         return False, "Moyen"
#     else:
#         return False, "Faible"

# # ============================================
# # STYLES CSS (Bleu, Vert, Blanc)
# # ============================================

# app.index_string = '''
# <!DOCTYPE html>
# <html>
#     <head>
#         {%metas%}
#         <title>{%title%}</title>
#         {%favicon%}
#         {%css%}
#         <style>
#             @import url('https://fonts.googleapis.com/css2?family=Inter:wght@100..900&display=swap');
            
#             * {
#                 margin: 0;
#                 padding: 0;
#                 box-sizing: border-box;
#             }
            
#             body {
#                 font-family: 'Inter', sans-serif;
#                 background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
#                 min-height: 100vh;
#                 display: flex;
#                 align-items: center;
#                 justify-content: center;
#             }
            
#             .auth-container {
#                 max-width: 500px;
#                 width: 90%;
#                 margin: 2rem auto;
#             }
            
#             .auth-card {
#                 background: white;
#                 border-radius: 32px;
#                 padding: 2.5rem;
#                 box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
#                 animation: fadeIn 0.5s ease-out;
#             }
            
#             @keyframes fadeIn {
#                 from {
#                     opacity: 0;
#                     transform: translateY(20px);
#                 }
#                 to {
#                     opacity: 1;
#                     transform: translateY(0);
#                 }
#             }
            
#             .logo-section {
#                 text-align: center;
#                 margin-bottom: 2rem;
#             }
            
#             .logo-icon {
#                 font-size: 3.5rem;
#                 animation: pulse 2s infinite;
#             }
            
#             @keyframes pulse {
#                 0%, 100% { transform: scale(1); }
#                 50% { transform: scale(1.05); }
#             }
            
#             .logo-title {
#                 font-size: 2rem;
#                 font-weight: 800;
#                 background: linear-gradient(135deg, #3B82F6, #10B981);
#                 -webkit-background-clip: text;
#                 -webkit-text-fill-color: transparent;
#                 margin-top: 0.5rem;
#             }
            
#             .logo-subtitle {
#                 color: #64748B;
#                 font-size: 0.875rem;
#                 margin-top: 0.5rem;
#             }
            
#             .tabs-container {
#                 display: flex;
#                 gap: 0.5rem;
#                 background: #F1F5F9;
#                 padding: 0.5rem;
#                 border-radius: 16px;
#                 margin-bottom: 2rem;
#             }
            
#             .tab-btn {
#                 flex: 1;
#                 padding: 0.75rem;
#                 border: none;
#                 background: transparent;
#                 font-weight: 600;
#                 font-size: 0.875rem;
#                 cursor: pointer;
#                 border-radius: 12px;
#                 transition: all 0.3s ease;
#                 color: #64748B;
#             }
            
#             .tab-btn.active {
#                 background: linear-gradient(135deg, #3B82F6, #10B981);
#                 color: white;
#                 box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
#             }
            
#             .form-group {
#                 margin-bottom: 1.5rem;
#             }
            
#             .form-label {
#                 display: block;
#                 margin-bottom: 0.5rem;
#                 color: #334155;
#                 font-weight: 600;
#                 font-size: 0.875rem;
#             }
            
#             .form-control {
#                 width: 100%;
#                 padding: 0.875rem 1rem;
#                 border: 2px solid #E2E8F0;
#                 border-radius: 14px;
#                 font-size: 0.95rem;
#                 transition: all 0.3s ease;
#                 background: #F8FAFC;
#             }
            
#             .form-control:focus {
#                 outline: none;
#                 border-color: #3B82F6;
#                 box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
#                 background: white;
#             }
            
#             .btn-primary {
#                 width: 100%;
#                 padding: 1rem;
#                 background: linear-gradient(135deg, #3B82F6, #10B981);
#                 color: white;
#                 border: none;
#                 border-radius: 14px;
#                 font-size: 1rem;
#                 font-weight: 700;
#                 cursor: pointer;
#                 transition: all 0.3s ease;
#             }
            
#             .btn-primary:hover {
#                 transform: translateY(-2px);
#                 box-shadow: 0 10px 25px -5px rgba(59, 130, 246, 0.5);
#             }
            
#             .message-success {
#                 background: #D1FAE5;
#                 color: #065F46;
#                 padding: 0.875rem;
#                 border-radius: 12px;
#                 margin-bottom: 1rem;
#                 border-left: 4px solid #10B981;
#                 font-weight: 500;
#                 animation: slideDown 0.3s ease-out;
#             }
            
#             .message-error {
#                 background: #FEE2E2;
#                 color: #991B1B;
#                 padding: 0.875rem;
#                 border-radius: 12px;
#                 margin-bottom: 1rem;
#                 border-left: 4px solid #EF4444;
#                 font-weight: 500;
#                 animation: slideDown 0.3s ease-out;
#             }
            
#             @keyframes slideDown {
#                 from {
#                     opacity: 0;
#                     transform: translateY(-10px);
#                 }
#                 to {
#                     opacity: 1;
#                     transform: translateY(0);
#                 }
#             }
            
#             .divider {
#                 text-align: center;
#                 margin: 2rem 0;
#                 position: relative;
#             }
            
#             .divider::before {
#                 content: "";
#                 position: absolute;
#                 left: 0;
#                 top: 50%;
#                 width: 100%;
#                 height: 1px;
#                 background: linear-gradient(90deg, transparent, #E2E8F0, transparent);
#             }
            
#             .divider span {
#                 background: white;
#                 padding: 0 1rem;
#                 position: relative;
#                 color: #94A3B8;
#                 font-size: 0.875rem;
#             }
            
#             .footer {
#                 text-align: center;
#                 margin-top: 2rem;
#                 padding-top: 1.5rem;
#                 border-top: 1px solid #E2E8F0;
#                 color: #94A3B8;
#                 font-size: 0.75rem;
#             }
#         </style>
#     </head>
#     <body>
#         {%app_entry%}
#         <footer>
#             {%config%}
#             {%scripts%}
#             {%renderer%}
#         </footer>
#     </body>
# </html>
# '''

# # ============================================
# # LAYOUT
# # ============================================

# app.layout = html.Div([
#     dcc.Store(id='auth-store', data={'authenticated': False, 'username': None}),
#     dcc.Store(id='active-tab', data='login'),
#     dcc.Location(id='url', refresh=False),
    
#     html.Div([
#         html.Div([
#             html.Div([
#                 # Logo
#                 html.Div([
#                     html.Div("📡", className="logo-icon"),
#                     html.Div("Télécom DZ", className="logo-title"),
#                     html.Div("Analyse intelligente des sentiments", className="logo-subtitle"),
#                 ], className="logo-section"),
                
#                 # Tabs
#                 html.Div([
#                     html.Button("🔐 CONNEXION", id="login-tab", className="tab-btn active", n_clicks=0),
#                     html.Button("📝 INSCRIPTION", id="signup-tab", className="tab-btn", n_clicks=0),
#                 ], className="tabs-container"),
                
#                 # Message
#                 html.Div(id="auth-message"),
                
#                 # Formulaire Connexion
#                 html.Div(id="login-form", children=[
#                     html.Div([
#                         html.Label("📧 Email", className="form-label"),
#                         dcc.Input(id="login-email", type="email", className="form-control", placeholder="vous@entreprise.com"),
#                     ], className="form-group"),
                    
#                     html.Div([
#                         html.Label("🔒 Mot de passe", className="form-label"),
#                         dcc.Input(id="login-password", type="password", className="form-control", placeholder="Votre mot de passe"),
#                     ], className="form-group"),
                    
#                     html.Button("Se connecter", id="login-btn", className="btn-primary", n_clicks=0),
#                 ]),
                
#                 # Formulaire Inscription
#                 html.Div(id="signup-form", style={"display": "none"}, children=[
#                     html.Div([
#                         html.Label("👤 Nom complet", className="form-label"),
#                         dcc.Input(id="signup-name", type="text", className="form-control", placeholder="Jean Dupont"),
#                     ], className="form-group"),
                    
#                     html.Div([
#                         html.Label("📧 Email", className="form-label"),
#                         dcc.Input(id="signup-email", type="email", className="form-control", placeholder="jean@entreprise.com"),
#                     ], className="form-group"),
                    
#                     html.Div([
#                         html.Label("🔒 Mot de passe", className="form-label"),
#                         dcc.Input(id="signup-password", type="password", className="form-control", placeholder="8+ caractères, 1 majuscule, 1 chiffre"),
#                     ], className="form-group"),
                    
#                     html.Div([
#                         html.Label("🔒 Confirmer mot de passe", className="form-label"),
#                         dcc.Input(id="signup-confirm", type="password", className="form-control", placeholder="Retapez votre mot de passe"),
#                     ], className="form-group"),
                    
#                     html.Button("Créer mon compte", id="signup-btn", className="btn-primary", n_clicks=0),
#                 ]),
                
#                 # Divider
#                 html.Div([
#                     html.Div(html.Span("Ou continuer avec"), className="divider")
#                 ]),
                
#                 # Boutons sociaux
#                 html.Div([
#                     html.Button("🌐 Google", className="btn-primary", style={"background": "#DB4437", "marginBottom": "0.5rem"}),
#                     html.Button("💼 LinkedIn", className="btn-primary", style={"background": "#0077B5", "marginBottom": "0.5rem"}),
#                     html.Button("🐙 GitHub", className="btn-primary", style={"background": "#333"}),
#                 ]),
                
#                 # Footer
#                 html.Div([
#                     html.Div("© 2026 Télécom DZ - Analyse des sentiments"),
#                     html.Div(style={"marginTop": "0.5rem"}, children=[
#                         html.Span("🔵 Bleu · ", style={"color": "#3B82F6"}),
#                         html.Span("🟢 Vert · ", style={"color": "#10B981"}),
#                         html.Span("⚪ Blanc", style={"color": "#000000"}),
#                     ]),
#                 ], className="footer"),
                
#             ], className="auth-card"),
#         ], className="auth-container"),
#     ]),
# ])

# # ============================================
# # CALLBACKS
# # ============================================

# @app.callback(
#     [Output("login-form", "style"),
#      Output("signup-form", "style"),
#      Output("login-tab", "className"),
#      Output("signup-tab", "className"),
#      Output("active-tab", "data")],
#     [Input("login-tab", "n_clicks"),
#      Input("signup-tab", "n_clicks")],
#     prevent_initial_call=True
# )
# def switch_tabs(login_clicks, signup_clicks):
#     ctx = callback_context
#     if not ctx.triggered:
#         return {}, {"display": "none"}, "tab-btn active", "tab-btn", "login"
    
#     button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
#     if button_id == "login-tab":
#         return {}, {"display": "none"}, "tab-btn active", "tab-btn", "login"
#     else:
#         return {"display": "none"}, {}, "tab-btn", "tab-btn active", "signup"

# @app.callback(
#     [Output("auth-message", "children"),
#      Output("auth-store", "data"),
#      Output("url", "pathname")],
#     [Input("login-btn", "n_clicks"),
#      Input("signup-btn", "n_clicks")],
#     [State("login-email", "value"),
#      State("login-password", "value"),
#      State("signup-name", "value"),
#      State("signup-email", "value"),
#      State("signup-password", "value"),
#      State("signup-confirm", "value"),
#      State("active-tab", "data"),
#      State("auth-store", "data")],
#     prevent_initial_call=True
# )
# def handle_auth(login_clicks, signup_clicks, login_email, login_password,
#                 signup_name, signup_email, signup_password, signup_confirm,
#                 active_tab, auth_data):
    
#     ctx = callback_context
#     if not ctx.triggered:
#         return "", auth_data, "/"
    
#     button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
#     # LOGIN - REDIRECTION VERS /analyse
#     if button_id == "login-btn" and login_clicks > 0:
#         if not login_email or not login_password:
#             return html.Div("❌ Veuillez remplir tous les champs", className="message-error"), auth_data, "/pages/analyse"
        
#         users = load_users()
#         hashed = hash_password(login_password)
        
#         if login_email in users and users[login_email]['password'] == hashed:
#             auth_data['authenticated'] = True
#             auth_data['username'] = users[login_email]['name']
#             return html.Div(f"✅ Bienvenue {users[login_email]['name']} !", className="message-success"), auth_data, "/analyse"
#         else:
#             return html.Div("❌ Email ou mot de passe incorrect", className="message-error"), auth_data, "/"
    
#     # SIGNUP
#     elif button_id == "signup-btn" and signup_clicks > 0:
#         if not signup_name or not signup_email or not signup_password:
#             return html.Div("❌ Veuillez remplir tous les champs", className="message-error"), auth_data, "/"
        
#         if not validate_email(signup_email):
#             return html.Div("❌ Email invalide", className="message-error"), auth_data, "/"
        
#         if signup_password != signup_confirm:
#             return html.Div("❌ Les mots de passe ne correspondent pas", className="message-error"), auth_data, "/"
        
#         is_valid, strength = validate_password(signup_password)
#         if not is_valid:
#             return html.Div(f"❌ Mot de passe trop faible (Force: {strength})", className="message-error"), auth_data, "/"
        
#         users = load_users()
#         if signup_email in users:
#             return html.Div("❌ Cet email est déjà utilisé", className="message-error"), auth_data, "/"
        
#         users[signup_email] = {
#             'name': signup_name,
#             'email': signup_email,
#             'password': hash_password(signup_password),
#             'created_at': datetime.now().isoformat()
#         }
#         save_users(users)
        
#         return html.Div("✅ Compte créé avec succès ! Connectez-vous", className="message-success"), auth_data, "/pages/analyse"
    
#     return "", auth_data, "/pages/analyse"

# # ============================================
# # LANCEMENT
# # ============================================

# if __name__ == '__main__':
#     app.run(debug=True, port=8050)


# app.py — Page d'accueil principale
import streamlit as st
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from pages.style import THEME_CSS

st.set_page_config(
    page_title="Télécom DZ — Dashboard",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(THEME_CSS, unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:.5rem 0 1.2rem;">
        <div style="font-size:1.2rem;font-weight:800;color:#f1f5f9;">📡 TélécomDZ</div>
        <div style="font-size:.68rem;color:#475569;letter-spacing:.1em;text-transform:uppercase;font-weight:600;">Analyse des sentiments</div>
    </div>""", unsafe_allow_html=True)
    st.divider()
    st.markdown("""
    <div style="font-size:.72rem;color:#475569;padding:.4rem 0 .8rem;">NAVIGATION</div>
    """, unsafe_allow_html=True)
    st.success("✅ Dashboard prêt !")
    st.info("Utilisez le menu ci-dessus pour naviguer")
    st.divider()
    st.markdown("""
    <div style="font-size:.72rem;color:#374151;">
        <div style="margin-bottom:.4rem;">📊 <b style="color:#94a3b8;">Base</b> : telecom_algerie</div>
        <div style="margin-bottom:.4rem;">🔄 <b style="color:#94a3b8;">TTL cache</b> : 1 heure</div>
        <div>📅 <b style="color:#94a3b8;">Mise à jour</b> : Avril 2026</div>
    </div>""", unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
    border: 1px solid #1e2130;
    border-radius: 20px;
    padding: 3rem 2.5rem;
    text-align: center;
    margin-bottom: 2rem;
    position: relative;
    overflow: hidden;
">
    <div style="
        position: absolute; top: -60px; right: -60px;
        width: 200px; height: 200px; border-radius: 50%;
        background: radial-gradient(circle, rgba(59,130,246,.15), transparent 70%);
    "></div>
    <div style="
        position: absolute; bottom: -60px; left: -60px;
        width: 200px; height: 200px; border-radius: 50%;
        background: radial-gradient(circle, rgba(16,185,129,.1), transparent 70%);
    "></div>
    <div style="font-size:3rem;margin-bottom:1rem;">📡</div>
    <h1 style="
        font-size: 2.2rem; font-weight: 800; margin: 0 0 .5rem 0;
        background: linear-gradient(135deg, #60a5fa, #10b981);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    ">Dashboard Télécom Algérie</h1>
    <p style="color:#64748b;font-size:1rem;margin:0;">
        Plateforme d'analyse des sentiments clients · Opérateurs télécoms algériens
    </p>
</div>
""", unsafe_allow_html=True)

# ── Pages cards ───────────────────────────────────────────────────────────────
pages = [
    ("📊", "Vue d'ensemble", "KPIs, distributions et commentaires récents", "#3b82f6", "1_Overview"),
    ("📈", "Analyse",        "Évolutions temporelles, corrélations et filtres avancés", "#10b981", "2_Analyse"),
    ("💬", "Commentaires",   "Exploration et recherche textuelle dans les commentaires", "#8b5cf6", "3_Commentaires"),
    ("🔬", "Statistiques",   "Tests χ², t-test, violin plots et descriptifs complets", "#f59e0b", "4_Statistiques"),
    ("🤖", "ChatBot IA",     "Conversation intelligente · LLaMA 3 70B via Groq", "#ef4444", "5_ChatBot"),
]

cols = st.columns(len(pages))
for col, (icon, title, desc, color, page) in zip(cols, pages):
    with col:
        st.markdown(f"""
        <div style="
            background: #1a1d2e;
            border: 1px solid #1e2130;
            border-top: 3px solid {color};
            border-radius: 16px;
            padding: 1.4rem 1.2rem;
            text-align: center;
            transition: transform .2s;
            cursor: pointer;
        " onmouseover="this.style.transform='translateY(-4px)'" onmouseout="this.style.transform='translateY(0)'">
            <div style="font-size:2rem;margin-bottom:.6rem;">{icon}</div>
            <div style="font-size:.92rem;font-weight:700;color:#f1f5f9;margin-bottom:.4rem;">{title}</div>
            <div style="font-size:.72rem;color:#475569;line-height:1.5;">{desc}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Fonctionnalités & Stack ───────────────────────────────────────────────────
f1, f2, f3 = st.columns(3)

with f1:
    st.markdown("""
    <div class="card">
        <div class="card-title">🚀 Fonctionnalités</div>
        <br>
        <div style="font-size:.82rem;color:#94a3b8;line-height:2;">
            ✅ &nbsp; Analyse des sentiments positif/négatif<br>
            ✅ &nbsp; Visualisations interactives (Plotly)<br>
            ✅ &nbsp; Filtres dynamiques par source et période<br>
            ✅ &nbsp; Export CSV des données<br>
            ✅ &nbsp; Tests statistiques (χ², t-test)<br>
            ✅ &nbsp; ChatBot IA multilingue (FR/AR/DZ)
        </div>
    </div>""", unsafe_allow_html=True)

with f2:
    st.markdown("""
    <div class="card">
        <div class="card-title">📦 Stack technique</div>
        <br>
        <div style="font-size:.82rem;color:#94a3b8;line-height:2;">
            🐍 &nbsp; Python + Streamlit<br>
            🍃 &nbsp; MongoDB (base telecom_algerie)<br>
            📊 &nbsp; Plotly + Pandas + SciPy<br>
            🤖 &nbsp; Groq API · LLaMA 3 70B<br>
            🐳 &nbsp; Docker Compose<br>
            ⚡ &nbsp; Apache Spark (traitement distribué)
        </div>
    </div>""", unsafe_allow_html=True)

with f3:
    st.markdown("""
    <div class="card">
        <div class="card-title">📊 Sources de données</div>
        <br>
        <div style="font-size:.82rem;color:#94a3b8;line-height:2;">
            📘 &nbsp; Facebook — commentaires publics<br>
            🐦 &nbsp; Twitter / X — tweets<br>
            💬 &nbsp; Forums — discussions<br>
            ✍️ &nbsp; Annotations manuelles<br>
            🔄 &nbsp; Pipeline Spark distribué<br>
            🗄️ &nbsp; Unifiés dans dataset_unifie
        </div>
    </div>""", unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;padding:2rem 0 1rem;color:#374151;font-size:.73rem;">
    Télécom DZ Analytics · Avril 2026 · Powered by Streamlit + Groq
</div>""", unsafe_allow_html=True)