# app.py - Application Dash complète avec authentification et analyse
import dash
from dash import dcc, html, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hashlib
import re
import json
import os

# ============================================
# CONFIGURATION
# ============================================

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Télécom DZ - Analyse des sentiments"
)

server = app.server

# ============================================
# CONNEXION MONGODB
# ============================================

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/")

def get_mongo_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"Erreur MongoDB: {e}")
        return None

# ============================================
# GESTION DES UTILISATEURS
# ============================================

USERS_FILE = "users.json"

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=2)

def validate_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_password(password):
    score = 0
    if len(password) >= 8:
        score += 1
    if re.search(r'[A-Z]', password):
        score += 1
    if re.search(r'[a-z]', password):
        score += 1
    if re.search(r'[0-9]', password):
        score += 1
    
    if score >= 3:
        return True, "Fort", score
    elif score >= 2:
        return False, "Moyen", score
    else:
        return False, "Faible", score

# ============================================
# STYLES CSS (Bleu, Vert, Blanc)
# ============================================

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@100..900&display=swap');
            
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: 'Inter', sans-serif;
                background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
                min-height: 100vh;
            }
            
            /* Cartes */
            .card-custom {
                background: white;
                border-radius: 20px;
                padding: 1.5rem;
                margin-bottom: 1.5rem;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                transition: transform 0.2s;
            }
            .card-custom:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
            }
            
            /* KPI Cards */
            .kpi-card {
                background: white;
                border-radius: 16px;
                padding: 1.25rem;
                text-align: center;
                border-left: 4px solid #3B82F6;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }
            .kpi-value {
                font-size: 2rem;
                font-weight: 800;
                color: #1E293B;
            }
            .kpi-label {
                font-size: 0.875rem;
                color: #64748B;
                margin-top: 0.5rem;
            }
            
            /* Boutons */
            .btn-primary-custom {
                background: linear-gradient(135deg, #3B82F6, #10B981);
                color: white;
                border: none;
                border-radius: 12px;
                padding: 0.75rem 1.5rem;
                font-weight: 600;
                transition: all 0.3s;
            }
            .btn-primary-custom:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 20px rgba(59,130,246,0.3);
            }
            
            /* Sidebar */
            .sidebar-custom {
                background: white;
                border-radius: 20px;
                padding: 1.5rem;
                margin: 1rem;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            }
            
            /* Filtres */
            .filter-group {
                margin-bottom: 1.5rem;
            }
            .filter-label {
                font-weight: 600;
                color: #334155;
                margin-bottom: 0.5rem;
                font-size: 0.875rem;
            }
            
            /* Messages */
            .message-success {
                background: #D1FAE5;
                color: #065F46;
                padding: 0.75rem;
                border-radius: 12px;
                border-left: 4px solid #10B981;
                margin-bottom: 1rem;
            }
            .message-error {
                background: #FEE2E2;
                color: #991B1B;
                padding: 0.75rem;
                border-radius: 12px;
                border-left: 4px solid #EF4444;
                margin-bottom: 1rem;
            }
            
            /* Tableau */
            .dataframe {
                width: 100%;
                border-collapse: collapse;
            }
            .dataframe th {
                background: #F8FAFC;
                padding: 12px;
                text-align: left;
                font-weight: 600;
                color: #1E293B;
            }
            .dataframe td {
                padding: 10px;
                border-bottom: 1px solid #E2E8F0;
            }
            
            /* Auth Card */
            .auth-container {
                max-width: 500px;
                margin: 2rem auto;
            }
            .auth-card {
                background: white;
                border-radius: 32px;
                padding: 2.5rem;
                box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
            }
            .logo-section {
                text-align: center;
                margin-bottom: 2rem;
            }
            .logo-icon { font-size: 3.5rem; }
            .logo-title {
                font-size: 2rem;
                font-weight: 800;
                background: linear-gradient(135deg, #3B82F6, #10B981);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .tabs-container {
                display: flex;
                gap: 0.5rem;
                background: #F1F5F9;
                padding: 0.5rem;
                border-radius: 16px;
                margin-bottom: 2rem;
            }
            .tab-btn {
                flex: 1;
                padding: 0.75rem;
                border: none;
                background: transparent;
                font-weight: 600;
                border-radius: 12px;
                transition: all 0.3s;
                color: #64748B;
            }
            .tab-btn.active {
                background: linear-gradient(135deg, #3B82F6, #10B981);
                color: white;
            }
            .form-control {
                width: 100%;
                padding: 0.875rem;
                border: 2px solid #E2E8F0;
                border-radius: 14px;
                font-size: 0.95rem;
            }
            .form-control:focus {
                outline: none;
                border-color: #3B82F6;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# ============================================
# FONCTIONS D'ANALYSE DES DONNÉES
# ============================================

def load_data(sentiment_filter=None, sources=None):
    """Charge les données depuis MongoDB"""
    client = get_mongo_connection()
    if client is None:
        return pd.DataFrame()
    
    db = client["telecom_algerie"]
    collection = db["dataset_unifie"]
    
    query = {}
    if sentiment_filter:
        query["label_final"] = {"$in": sentiment_filter}
    if sources:
        query["sources"] = {"$in": sources}
    
    data = list(collection.find(query).limit(5000))
    df = pd.DataFrame(data) if data else pd.DataFrame()
    
    if not df.empty and 'dates' in df.columns:
        try:
            df['date_parsed'] = pd.to_datetime(df['dates'], errors='coerce')
            df['date_only'] = df['date_parsed'].dt.date
        except:
            df['date_parsed'] = datetime.now()
    
    return df

def create_evolution_chart(df):
    """Graphique d'évolution temporelle"""
    if df.empty or 'date_parsed' not in df.columns:
        return go.Figure()
    
    daily_counts = df.groupby(['date_only', 'label_final']).size().reset_index(name='count')
    
    fig = go.Figure()
    
    for sentiment in ['positif', 'negatif']:
        data = daily_counts[daily_counts['label_final'] == sentiment]
        if not data.empty:
            color = '#10B981' if sentiment == 'positif' else '#EF4444'
            fig.add_trace(go.Scatter(
                x=data['date_only'],
                y=data['count'],
                name=sentiment,
                line=dict(color=color, width=2),
                mode='lines+markers'
            ))
    
    fig.update_layout(
        title="Évolution des sentiments dans le temps",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#1E293B'),
        xaxis_title="Date",
        yaxis_title="Nombre de commentaires",
        hovermode='x unified'
    )
    
    return fig

def create_pie_chart(df):
    """Camembert des sentiments"""
    if df.empty:
        return go.Figure()
    
    sentiment_counts = df['label_final'].value_counts()
    
    colors = {'positif': '#10B981', 'negatif': '#EF4444', 'neutre': '#6B7280'}
    pie_colors = [colors.get(s, '#3B82F6') for s in sentiment_counts.index]
    
    fig = go.Figure(data=[go.Pie(
        labels=sentiment_counts.index,
        values=sentiment_counts.values,
        marker=dict(colors=pie_colors),
        textinfo='label+percent',
        textposition='auto'
    )])
    
    fig.update_layout(
        title="Répartition des sentiments",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#1E293B')
    )
    
    return fig

def create_confidence_histogram(df):
    """Histogramme des scores de confiance"""
    if df.empty or 'confidence' not in df.columns:
        return go.Figure()
    
    fig = go.Figure()
    
    for sentiment, color in [('positif', '#10B981'), ('negatif', '#EF4444')]:
        data = df[df['label_final'] == sentiment]['confidence'].dropna()
        if not data.empty:
            fig.add_trace(go.Histogram(
                x=data,
                name=sentiment,
                marker_color=color,
                opacity=0.7,
                nbinsx=20
            ))
    
    fig.update_layout(
        title="Distribution des scores de confiance",
        barmode='overlay',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#1E293B'),
        xaxis_title="Score de confiance",
        yaxis_title="Fréquence"
    )
    
    return fig

def create_source_analysis(df):
    """Analyse par source"""
    if df.empty or 'sources' not in df.columns:
        return go.Figure()
    
    source_sentiment = pd.crosstab(df['sources'], df['label_final'])
    source_sentiment = source_sentiment.reset_index().melt(id_vars=['sources'], var_name='sentiment', value_name='count')
    
    fig = px.bar(
        source_sentiment,
        x='sources',
        y='count',
        color='sentiment',
        color_discrete_map={'positif': '#10B981', 'negatif': '#EF4444'},
        title="Sentiment par source",
        barmode='group'
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#1E293B')
    )
    
    return fig

# ============================================
# LAYOUT PRINCIPAL
# ============================================

app.layout = html.Div([
    dcc.Store(id='auth-store', data={'authenticated': False, 'username': None}),
    dcc.Store(id='active-tab', data='login'),
    dcc.Store(id='message-store', data={'type': None, 'text': None}),
    dcc.Location(id='url', refresh=False),
    
    # Conteneur principal
    html.Div(id='main-content')
])

# ============================================
# PAGE D'AUTHENTIFICATION
# ============================================

def create_auth_page():
    return html.Div([
        html.Div([
            html.Div([
                # Logo
                html.Div([
                    html.Div("📡", className="logo-icon"),
                    html.Div("Télécom DZ", className="logo-title"),
                    html.Div("Analyse intelligente des sentiments", className="logo-subtitle"),
                ], className="logo-section"),
                
                # Tabs
                html.Div([
                    html.Button("🔐 Connexion", id="login-tab", className="tab-btn active", n_clicks=0),
                    html.Button("📝 Inscription", id="signup-tab", className="tab-btn", n_clicks=0),
                ], className="tabs-container"),
                
                # Message
                html.Div(id="auth-message"),
                
                # Formulaire Connexion
                html.Div(id="login-form", children=[
                    html.Div([
                        html.Label("📧 Email", className="filter-label"),
                        dcc.Input(id="login-email", type="email", className="form-control", placeholder="vous@entreprise.com"),
                    ], className="filter-group"),
                    
                    html.Div([
                        html.Label("🔒 Mot de passe", className="filter-label"),
                        dcc.Input(id="login-password", type="password", className="form-control", placeholder="Votre mot de passe"),
                    ], className="filter-group"),
                    
                    html.Button("Se connecter", id="login-btn", className="btn-primary-custom", style={"width": "100%"}),
                ]),
                
                # Formulaire Inscription
                html.Div(id="signup-form", style={"display": "none"}, children=[
                    html.Div([
                        html.Label("👤 Nom complet", className="filter-label"),
                        dcc.Input(id="signup-name", type="text", className="form-control", placeholder="Jean Dupont"),
                    ], className="filter-group"),
                    
                    html.Div([
                        html.Label("📧 Email", className="filter-label"),
                        dcc.Input(id="signup-email", type="email", className="form-control", placeholder="jean@entreprise.com"),
                    ], className="filter-group"),
                    
                    html.Div([
                        html.Label("🔒 Mot de passe", className="filter-label"),
                        dcc.Input(id="signup-password", type="password", className="form-control", placeholder="Créez un mot de passe"),
                    ], className="filter-group"),
                    
                    html.Div([
                        html.Label("🔒 Confirmer mot de passe", className="filter-label"),
                        dcc.Input(id="signup-confirm", type="password", className="form-control", placeholder="Retapez votre mot de passe"),
                    ], className="filter-group"),
                    
                    html.Button("Créer mon compte", id="signup-btn", className="btn-primary-custom", style={"width": "100%"}),
                ]),
                
            ], className="auth-card"),
        ], className="auth-container"),
    ])

# ============================================
# PAGE D'ANALYSE
# ============================================

def create_analysis_page(username):
    return html.Div([
        # Sidebar avec filtres
        html.Div([
            html.Div([
                html.H3("📡 Télécom DZ", style={"color": "#3B82F6", "marginBottom": "0.5rem"}),
                html.P(f"👤 Connecté: {username}", style={"color": "#64748B", "marginBottom": "1.5rem"}),
                html.Hr(),
                
                html.H4("🔍 Filtres", style={"marginBottom": "1rem"}),
                
                html.Div([
                    html.Label("Sentiment", className="filter-label"),
                    dcc.Dropdown(
                        id="filter-sentiment",
                        options=[
                            {"label": "😊 Positif", "value": "positif"},
                            {"label": "😠 Négatif", "value": "negatif"}
                        ],
                        value=["positif", "negatif"],
                        multi=True,
                        className="form-control",
                        style={"marginBottom": "1rem"}
                    ),
                ]),
                
                html.Div([
                    html.Label("Source", className="filter-label"),
                    dcc.Dropdown(
                        id="filter-source",
                        options=[
                            {"label": "Facebook", "value": "Facebook"},
                            {"label": "Twitter", "value": "Twitter"},
                            {"label": "Forum", "value": "Forum"}
                        ],
                        value=["Facebook", "Twitter", "Forum"],
                        multi=True,
                        className="form-control",
                        style={"marginBottom": "1rem"}
                    ),
                ]),
                
                html.Hr(),
                html.Button("🚪 Se déconnecter", id="logout-btn", className="btn-primary-custom", style={"width": "100%", "background": "#EF4444"}),
                
            ], className="sidebar-custom"),
        ], style={"position": "fixed", "width": "280px", "left": "0", "top": "0", "height": "100vh", "overflowY": "auto"}),
        
        # Contenu principal
        html.Div([
            html.Div([
                html.H1("📈 Analyse approfondie", style={"color": "#1E293B", "marginBottom": "0.5rem"}),
                html.P("Analyse des sentiments des commentaires télécom", style={"color": "#64748B", "marginBottom": "2rem"}),
                
                # KPI
                html.Div(id="kpi-cards", className="row", style={"display": "flex", "gap": "1rem", "marginBottom": "2rem"}),
                
                # Graphique évolution
                html.Div([
                    dcc.Graph(id="evolution-chart")
                ], className="card-custom"),
                
                # Deux colonnes
                html.Div([
                    html.Div([
                        dcc.Graph(id="pie-chart")
                    ], className="card-custom", style={"width": "48%", "display": "inline-block"}),
                    html.Div([
                        dcc.Graph(id="confidence-histogram")
                    ], className="card-custom", style={"width": "48%", "display": "inline-block", "marginLeft": "4%"}),
                ], style={"display": "flex", "marginBottom": "1.5rem"}),
                
                # Analyse par source
                html.Div([
                    dcc.Graph(id="source-analysis")
                ], className="card-custom"),
                
                # Tableau des données
                html.Div([
                    html.H3("📋 Détail des commentaires", style={"marginBottom": "1rem"}),
                    html.Div(id="data-table"),
                    html.Button("📥 Exporter CSV", id="export-btn", className="btn-primary-custom", style={"marginTop": "1rem"})
                ], className="card-custom"),
                
                dcc.Download(id="download-csv"),
                
            ], style={"marginLeft": "320px", "padding": "2rem"}),
        ]),
        
        # Intervalle pour mise à jour
        dcc.Interval(id="interval-component", interval=5000, n_intervals=0),
    ])

# ============================================
# CALLBACKS
# ============================================

@app.callback(
    Output("main-content", "children"),
    [Input("url", "pathname"),
     Input("auth-store", "data")]
)
def display_page(pathname, auth_data):
    if auth_data and auth_data.get('authenticated'):
        return create_analysis_page(auth_data.get('username', 'Utilisateur'))
    else:
        return create_auth_page()

@app.callback(
    [Output("login-form", "style"),
     Output("signup-form", "style"),
     Output("login-tab", "className"),
     Output("signup-tab", "className"),
     Output("active-tab", "data")],
    [Input("login-tab", "n_clicks"),
     Input("signup-tab", "n_clicks")],
    prevent_initial_call=True
)
def switch_tabs(login_clicks, signup_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return {}, {"display": "none"}, "tab-btn active", "tab-btn", "login"
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == "login-tab":
        return {}, {"display": "none"}, "tab-btn active", "tab-btn", "login"
    else:
        return {"display": "none"}, {}, "tab-btn", "tab-btn active", "signup"

@app.callback(
    Output("auth-message", "children"),
    [Input("login-btn", "n_clicks"),
     Input("signup-btn", "n_clicks")],
    [State("login-email", "value"),
     State("login-password", "value"),
     State("signup-name", "value"),
     State("signup-email", "value"),
     State("signup-password", "value"),
     State("signup-confirm", "value"),
     State("active-tab", "data"),
     State("auth-store", "data")],
    prevent_initial_call=True
)
def handle_auth(login_clicks, signup_clicks, login_email, login_password,
                signup_name, signup_email, signup_password, signup_confirm,
                active_tab, auth_data):
    
    ctx = callback_context
    if not ctx.triggered:
        return ""
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # LOGIN
    if button_id == "login-btn" and login_clicks > 0:
        if not login_email or not login_password:
            return html.Div("❌ Veuillez remplir tous les champs", className="message-error")
        
        users = load_users()
        hashed = hash_password(login_password)
        
        if login_email in users and users[login_email]['password'] == hashed:
            auth_data['authenticated'] = True
            auth_data['username'] = users[login_email]['name']
            return html.Div(f"✅ Bienvenue {users[login_email]['name']} ! Redirection...", className="message-success")
        else:
            return html.Div("❌ Email ou mot de passe incorrect", className="message-error")
    
    # SIGNUP
    elif button_id == "signup-btn" and signup_clicks > 0:
        if not signup_name or not signup_email or not signup_password:
            return html.Div("❌ Veuillez remplir tous les champs", className="message-error")
        
        if not validate_email(signup_email):
            return html.Div("❌ Email invalide", className="message-error")
        
        if signup_password != signup_confirm:
            return html.Div("❌ Les mots de passe ne correspondent pas", className="message-error")
        
        is_valid, strength, score = validate_password(signup_password)
        if not is_valid:
            return html.Div(f"❌ Mot de passe trop faible (Force: {strength})", className="message-error")
        
        users = load_users()
        if signup_email in users:
            return html.Div("❌ Cet email est déjà utilisé", className="message-error")
        
        users[signup_email] = {
            'name': signup_name,
            'email': signup_email,
            'password': hash_password(signup_password),
            'created_at': datetime.now().isoformat()
        }
        save_users(users)
        
        return html.Div("✅ Compte créé avec succès ! Connectez-vous", className="message-success")
    
    return ""

@app.callback(
    [Output("kpi-cards", "children"),
     Output("evolution-chart", "figure"),
     Output("pie-chart", "figure"),
     Output("confidence-histogram", "figure"),
     Output("source-analysis", "figure"),
     Output("data-table", "children")],
    [Input("interval-component", "n_intervals"),
     Input("filter-sentiment", "value"),
     Input("filter-source", "value")]
)
def update_dashboard(n_intervals, sentiment_filter, source_filter):
    # Charger les données
    df = load_data(sentiment_filter, source_filter)
    
    if df.empty:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Aucune donnée disponible",
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        return [], empty_fig, empty_fig, empty_fig, empty_fig, html.Div("Aucune donnée")
    
    # KPI Cards
    total = len(df)
    positif = len(df[df['label_final'] == 'positif'])
    negatif = len(df[df['label_final'] == 'negatif'])
    taux_positif = (positif / total * 100) if total > 0 else 0
    sources_count = df['sources'].nunique() if 'sources' in df else 0
    
    kpis = html.Div([
        html.Div([
            html.Div("📝", style={"fontSize": "2rem"}),
            html.Div(f"{total:,}", className="kpi-value"),
            html.Div("Total commentaires", className="kpi-label"),
        ], className="kpi-card", style={"flex": "1"}),
        
        html.Div([
            html.Div("😊", style={"fontSize": "2rem"}),
            html.Div(f"{taux_positif:.1f}%", className="kpi-value"),
            html.Div("Taux positif", className="kpi-label"),
        ], className="kpi-card", style={"flex": "1"}),
        
        html.Div([
            html.Div("😠", style={"fontSize": "2rem"}),
            html.Div(f"{100-taux_positif:.1f}%", className="kpi-value"),
            html.Div("Taux négatif", className="kpi-label"),
        ], className="kpi-card", style={"flex": "1"}),
        
        html.Div([
            html.Div("📱", style={"fontSize": "2rem"}),
            html.Div(f"{sources_count}", className="kpi-value"),
            html.Div("Sources", className="kpi-label"),
        ], className="kpi-card", style={"flex": "1"}),
    ], style={"display": "flex", "gap": "1rem", "width": "100%"})
    
    # Graphiques
    evolution_fig = create_evolution_chart(df)
    pie_fig = create_pie_chart(df)
    confidence_fig = create_confidence_histogram(df)
    source_fig = create_source_analysis(df)
    
    # Tableau
    display_cols = ['Commentaire_Client', 'label_final', 'sources', 'dates', 'confidence']
    existing_cols = [c for c in display_cols if c in df.columns]
    
    table = html.Table([
        html.Thead(html.Tr([html.Th(col) for col in existing_cols])),
        html.Tbody([
            html.Tr([html.Td(str(row[col])[:50]) for col in existing_cols])
            for _, row in df.head(50).iterrows()
        ])
    ], className="dataframe")
    
    return kpis, evolution_fig, pie_fig, confidence_fig, source_fig, table

@app.callback(
    Output("download-csv", "data"),
    Input("export-btn", "n_clicks"),
    [State("filter-sentiment", "value"),
     State("filter-source", "value")],
    prevent_initial_call=True
)
def export_csv(n_clicks, sentiment_filter, source_filter):
    df = load_data(sentiment_filter, source_filter)
    if not df.empty:
        return dcc.send_data_frame(df.to_csv, f"analyse_telecom_{datetime.now().strftime('%Y%m%d')}.csv", index=False)
    return None

@app.callback(
    Output("auth-store", "data"),
    Input("logout-btn", "n_clicks"),
    prevent_initial_call=True
)
def logout(n_clicks):
    return {'authenticated': False, 'username': None}

# ============================================
# LANCEMENT
# ============================================
 
# LANCEMENT - AVEC GESTION D'ERREUR
# ============================================

if __name__ == '__main_':
    try:
        # Tentative avec debug=True
        app.run(debug=True, port=8050, use_reloader=False)
    except Exception as e:
        print(f"Erreur avec debug=True: {e}")
        print("Redémarrage avec debug=False...")
        app.run(debug=False, port=8050, host='0.0.0.0')
