import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import os
# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
def sidebar():
    with st.sidebar:
        # Profile
       # Version simplifiée du logo
        logo_path = "/home/mouna/projet_telecom/dash/Logo_Algérie_Télécom.png"

        if os.path.exists(logo_path):
            st.image(logo_path, width=180)
        else:
            st.markdown(
        """
        <div style="text-align:center; margin-bottom:20px;">
            <i class="fas fa-tower-cell" style="font-size:3rem; color:#38bdf8;"></i>
        </div>
        """,
        unsafe_allow_html=True,
    )

        # # Team selector
        # st.selectbox("Teams", ["Redwhale LLC", "Design Studio", "Freelance"], label_visibility="collapsed")
        # st.markdown("<div style='font-size:0.72rem;color:#bbb;margin-bottom:12px;'>Teams</div>", unsafe_allow_html=True)

        # Menu
        st.markdown("<div style='font-size:0.72rem;color:#bbb;font-weight:600;letter-spacing:0.05em;margin-bottom:8px;'>MENU</div>", unsafe_allow_html=True)

        pages = [
            ("📊", "Dashboard"),
            ("📥", "Inbox"),
            ("👤", "Accounts"),
            ("🔥", "Trendign"),
            ("📅", "Meetings"),
        ]

        for icon, name in pages:
            is_active = st.session_state.page == name
            style = "background:#fff5ef;color:#FF6B2B;font-weight:600;" if is_active else "color:#888;"
            badge = " <span style='background:#FF6B2B;color:white;border-radius:50%;padding:1px 6px;font-size:0.65rem;margin-left:auto;'>5</span>" if name == "Meetings" else ""
            if st.button(f"{icon}  {name}", key=f"nav_{name}", use_container_width=True):
                st.session_state.page = name
                st.rerun()

        st.markdown("<hr style='border:none;border-top:1px solid #f0f0f5;margin:12px 0;'>", unsafe_allow_html=True)
        st.markdown("<div style='font-size:0.72rem;color:#bbb;font-weight:600;letter-spacing:0.05em;margin-bottom:8px;'>SETTINGS & BEAT</div>", unsafe_allow_html=True)

        for icon, name in [("⚙️", "Settings"), ("🎵", "Daily Beat")]:
            if st.button(f"{icon}  {name}", key=f"nav2_{name}", use_container_width=True):
                st.session_state.page = name
                st.rerun()

        st.markdown("<hr style='border:none;border-top:1px solid #f0f0f5;margin:12px 0;'>", unsafe_allow_html=True)
        st.markdown("<div style='font-size:0.72rem;color:#bbb;font-weight:600;letter-spacing:0.05em;margin-bottom:8px;'>QUICK ACTIONS</div>", unsafe_allow_html=True)
        st.markdown("""
        <div style="display:flex;gap:10px;margin-bottom:16px;">
            <span style="cursor:pointer;font-size:1.1rem;">✏️</span>
            <span style="cursor:pointer;font-size:1.1rem;">🔗</span>
            <span style="cursor:pointer;font-size:1.1rem;">⚡</span>
            <span style="cursor:pointer;font-size:1.1rem;">⭐</span>
        </div>
        """, unsafe_allow_html=True)

        # Add Invoice card
        st.markdown("""
        <div style="border:2px dashed #e0e0f0;border-radius:14px;padding:1.2rem;text-align:center;margin-bottom:16px;">
            <div style="width:36px;height:36px;background:#FF6B2B;border-radius:50%;
                display:flex;align-items:center;justify-content:center;
                font-size:1.2rem;color:white;margin:0 auto 8px;">+</div>
            <div style="font-size:0.82rem;font-weight:600;color:#1a1a2e;">Add New Invoice</div>
            <div style="font-size:0.72rem;color:#bbb;">or use <span style="color:#FF6B2B;text-decoration:underline;cursor:pointer;">invite link</span></div>
        </div>
        """, unsafe_allow_html=True)

        # Dark/Light toggle
        mode = st.radio("", ["🌙 Dark", "☀️ Light"], horizontal=True, label_visibility="collapsed")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def make_bar_line_chart():
    months = ["01","02","03","04","05","06","07","08","09","10","11","12"]
    bars = [38,32,40,36,45,48,42,38,44,46,50,55]
    line = [30,35,32,38,36,42,40,35,40,44,46,52]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=months, y=bars,
        marker_color="#FF6B2B",
        marker_line_width=0,
        width=0.6,
        name="Revenue"
    ))
    fig.add_trace(go.Scatter(
        x=months, y=line,
        mode="lines",
        line=dict(color="#3D3B8E", width=2.5, shape="spline"),
        fill="tozeroy",
        fillcolor="rgba(61,59,142,0.07)",
        name="Trend"
    ))
    fig.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(t=10, b=10, l=10, r=10),
        height=220,
        showlegend=False,
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color="#bbb"), tickcolor="white"),
        yaxis=dict(showgrid=False, visible=False),
        bargap=0.35
    )
    return fig

def make_donut_chart(pct=50):
    fig = go.Figure(go.Pie(
        values=[pct, 100-pct],
        hole=0.72,
        marker_colors=["#22c55e", "#ec4899", "#e8e8f0"],
        textinfo="none",
        sort=False,
        direction="clockwise"
    ))
    fig.update_layout(
        margin=dict(t=10, b=10, l=10, r=10),
        height=150,
        showlegend=False,
        paper_bgcolor="white",
        annotations=[dict(text=f"<b>{pct}%</b>", x=0.5, y=0.5,
            font_size=18, font_color="#1a1a2e", showarrow=False)]
    )
    return fig

def make_gauge_chart():
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=70,
        gauge=dict(
            axis=dict(range=[0, 100], tickwidth=0, tickcolor="white", visible=False),
            bar=dict(color="#FF6B2B", thickness=0.25),
            bgcolor="white",
            borderwidth=0,
            steps=[dict(range=[0, 100], color="#f0f0f5")]
        )
    ))
    fig.update_layout(
        margin=dict(t=20, b=20, l=20, r=20),
        height=120,
        paper_bgcolor="white",
    )
    return fig

def make_profile_ring():
    fig = go.Figure(go.Pie(
        values=[85, 15],
        hole=0.82,
        marker_colors=["#FF6B2B", "#f0f0f5"],
        textinfo="none",
        sort=False,
    ))
    fig.update_layout(
        margin=dict(t=5, b=5, l=5, r=5),
        height=110,
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)"
    )
    return fig


# ─────────────────────────────────────────────
# DASHBOARD PAGE
# ─────────────────────────────────────────────
def main_dashboard():
    # Header
    col_title, col_search = st.columns([2, 1])
    with col_title:
        st.markdown("""
        <div class="section-title">Analytics</div>
        <div class="section-sub">Welcome back, Let's get back to work.</div>
        """, unsafe_allow_html=True)
    with col_search:
        st.text_input("", placeholder="🔍  Search Dashboard", label_visibility="collapsed")

    # Top 3 metric cards
    c1, c2, c3, c4 = st.columns([1, 1, 1, 0.01])
    metrics = [
        ("👥", "#3D3B8E", "Total Connections", "2,140", "+45%", 70),
        ("🛡️", "#FF6B2B", "Security", "100%", "+55%", 100),
        ("📋", "#22c55e", "Total Items", "140", "+15%", 40),
    ]
    for col, (icon, color, label, value, change, prog) in zip([c1, c2, c3], metrics):
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">
                    <div style="width:40px;height:40px;border-radius:50%;background:{color}22;
                        display:flex;align-items:center;justify-content:center;font-size:1.1rem;">{icon}</div>
                    <div>
                        <div class="stat-label">{label}</div>
                        <div class="stat-num">{value}</div>
                    </div>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width:{prog}%;background:{color};"></div>
                </div>
                <div class="stat-up" style="margin-top:6px;">▲ {change}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

    # Middle row: Chart + Financial + Profile
    left, mid, right = st.columns([2.2, 0.9, 1.1])

    with left:
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:1.2rem 1.4rem;border:1px solid #f0f0f5;box-shadow:0 2px 12px rgba(0,0,0,0.04);">
        <div style="display:flex;gap:32px;margin-bottom:4px;">
            <div><div class="stat-label">Total Earnings</div><div class="stat-num">500k</div></div>
            <div><div class="stat-label">Period</div><div class="stat-num">1 Month</div></div>
            <div><div class="stat-label">Upcoming Projects</div><div class="stat-num">245</div></div>
        </div>
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(make_bar_line_chart(), use_container_width=True, config={"displayModeBar": False})

    with mid:
        for label, value in [("Personal Loans", "$45,000"), ("Subscriptions", "$495"), ("Income", "$15,000")]:
            st.markdown(f"""
            <div class="fin-card">
                <div class="fin-label">{label}</div>
                <div class="fin-value">{value}</div>
            </div>
            """, unsafe_allow_html=True)

    with right:
        # Profile card
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:1.4rem;border:1px solid #f0f0f5;text-align:center;">
            <div style="width:60px;height:60px;border-radius:50%;background:linear-gradient(135deg,#FF6B2B,#ff9a6b);
                display:flex;align-items:center;justify-content:center;
                font-size:1.6rem;margin:0 auto 8px;">🕶️</div>
            <div style="font-weight:700;font-size:1rem;color:#1a1a2e;">AR Shakir</div>
            <div style="font-size:0.75rem;color:#bbb;margin-bottom:12px;">Designer</div>
            <div style="display:flex;justify-content:space-around;">
                <div class="profile-stat">
                    <div class="profile-stat-num">457</div>
                    <div class="profile-stat-label">Projects</div>
                </div>
                <div class="profile-stat">
                    <div class="profile-stat-num">450</div>
                    <div class="profile-stat-label">Completed</div>
                </div>
                <div class="profile-stat">
                    <div class="profile-stat-num">12</div>
                    <div class="profile-stat-label">Awards</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

        for icon, bg, label in [("🎯", "#fff0e8", "Goals"), ("📅", "#fff0f5", "Monthly Plan"), ("⚙️", "#f0f5ff", "Settings")]:
            st.markdown(f"""
            <div class="action-row">
                <div style="display:flex;align-items:center;gap:10px;">
                    <div class="action-icon" style="background:{bg};">{icon}</div>
                    <span style="font-size:0.88rem;font-weight:600;color:#1a1a2e;">{label}</span>
                </div>
                <span style="color:#bbb;">›</span>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

    # Bottom row: Customers + Professional Status + Project Status
    b1, b2, b3 = st.columns([1.3, 1.1, 0.9])

    with b1:
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:1.2rem 1.4rem;border:1px solid #f0f0f5;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
            <span style="font-weight:700;font-size:0.95rem;color:#1a1a2e;">Latest Customers</span>
            <span style="font-size:0.78rem;color:#FF6B2B;cursor:pointer;">View All</span>
        </div>
        """, unsafe_allow_html=True)

        customers = [
            ("👱", "Harry Joe", "24 Purchases | 123 Likes"),
            ("👩🏽", "Martha June", "24 Purchases | 123 Likes"),
            ("👩🏻", "Sarah Altman", "24 Purchases | 123 Likes"),
            ("👨🏾", "Michael Clark", "24 Purchases | 123 Likes"),
        ]
        for icon, name, sub in customers:
            st.markdown(f"""
            <div class="customer-row">
                <div style="display:flex;align-items:center;gap:10px;">
                    <div style="width:36px;height:36px;border-radius:50%;background:#f5f5ff;
                        display:flex;align-items:center;justify-content:center;font-size:1.1rem;">{icon}</div>
                    <div>
                        <div class="customer-name">{name}</div>
                        <div class="customer-sub">{sub}</div>
                    </div>
                </div>
                <span style="font-size:1rem;color:#ddd;cursor:pointer;">✉️</span>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

    with b2:
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:1.2rem 1.4rem;border:1px solid #f0f0f5;">
        <div style="font-weight:700;font-size:0.95rem;color:#1a1a2e;margin-bottom:10px;">Your Professional Status</div>
        <div style="font-size:0.75rem;color:#bbb;">Current Level</div>
        <div style="font-weight:700;font-size:0.95rem;color:#1a1a2e;margin-bottom:8px;">Expert</div>
        <div style="font-size:0.75rem;color:#bbb;">Spent</div>
        <div style="font-weight:700;font-size:0.95rem;color:#1a1a2e;margin-bottom:12px;">$1,550 / $3,500</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(make_donut_chart(50), use_container_width=True, config={"displayModeBar": False})
        st.markdown("""
        <div class="sub-banner">🚀 You'll get new Subscription offers</div>
        </div>
        """, unsafe_allow_html=True)

    with b3:
        st.markdown("""
        <div style="background:white;border-radius:16px;padding:1.2rem 1.4rem;border:1px solid #f0f0f5;">
        <div style="font-weight:700;font-size:0.95rem;color:#1a1a2e;margin-bottom:4px;">Project Status</div>
        <div style="display:flex;align-items:baseline;gap:6px;">
            <span style="font-size:2rem;font-weight:800;color:#1a1a2e;">121</span>
            <span style="font-size:0.9rem;color:#ddd;">/ 143</span>
        </div>
        <div style="font-size:0.72rem;color:#bbb;margin-bottom:8px;">tasks completed</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(make_gauge_chart(), use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# INBOX PAGE
# ─────────────────────────────────────────────
def inbox_page():
    st.markdown('<div class="section-title">Inbox</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Your messages and notifications</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["📬 All", "⭐ Starred", "🗑️ Trash"])

    messages = [
        ("👱", "Harry Joe", "New project proposal", "Can we discuss the new design system?", "2m ago", True),
        ("👩🏽", "Martha June", "Invoice #1042", "Please review the attached invoice.", "14m ago", False),
        ("👩🏻", "Sarah Altman", "Meeting tomorrow", "Just confirming our 10am meeting.", "1h ago", True),
        ("👨🏾", "Michael Clark", "Feedback on mockups", "Loved the new color palette!", "3h ago", False),
        ("🧑", "Tom Baker", "Q3 Report", "The Q3 analytics report is ready.", "Yesterday", False),
        ("👩", "Anna Smith", "Welcome to Redwhale!", "We're excited to have you onboard.", "2d ago", False),
    ]

    with tab1:
        for icon, sender, subject, preview, time, unread in messages:
            bg = "#fffaf7" if unread else "white"
            dot = "🔵 " if unread else ""
            st.markdown(f"""
            <div style="background:{bg};border-radius:12px;padding:1rem 1.2rem;
                border:1px solid #f0f0f5;margin-bottom:8px;display:flex;gap:12px;align-items:flex-start;">
                <div style="font-size:1.4rem;">{icon}</div>
                <div style="flex:1;">
                    <div style="display:flex;justify-content:space-between;">
                        <span style="font-weight:{"700" if unread else "600"};font-size:0.88rem;color:#1a1a2e;">{dot}{sender}</span>
                        <span style="font-size:0.72rem;color:#bbb;">{time}</span>
                    </div>
                    <div style="font-size:0.82rem;font-weight:600;color:#444;margin-top:2px;">{subject}</div>
                    <div style="font-size:0.78rem;color:#bbb;">{preview}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    with tab2:
        for icon, sender, subject, preview, time, unread in messages:
            if unread:
                st.markdown(f"""
                <div style="background:white;border-radius:12px;padding:1rem 1.2rem;
                    border:1px solid #f0f0f5;margin-bottom:8px;">
                    <div style="font-weight:700;font-size:0.88rem;color:#1a1a2e;">⭐ {sender} — {subject}</div>
                    <div style="font-size:0.78rem;color:#bbb;">{preview}</div>
                </div>
                """, unsafe_allow_html=True)

    with tab3:
        st.info("Trash is empty.")


# ─────────────────────────────────────────────
# ACCOUNTS PAGE
# ─────────────────────────────────────────────
def accounts_page():
    st.markdown('<div class="section-title">Accounts</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Manage your connected accounts</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    accounts = [
        ("🏦", "Chase Bank", "**** 4821", "$24,500.00", "#22c55e"),
        ("💳", "PayPal", "**** 9032", "$3,200.00", "#3D3B8E"),
        ("🏧", "Stripe", "**** 1104", "$8,750.00", "#FF6B2B"),
        ("💰", "Wise", "**** 7733", "$1,200.00", "#22c55e"),
    ]

    for i, (icon, name, acc, balance, color) in enumerate(accounts):
        col = col1 if i % 2 == 0 else col2
        with col:
            st.markdown(f"""
            <div style="background:white;border-radius:16px;padding:1.4rem;border:1px solid #f0f0f5;
                margin-bottom:12px;box-shadow:0 2px 12px rgba(0,0,0,0.04);">
                <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px;">
                    <div style="width:44px;height:44px;border-radius:12px;background:{color}22;
                        display:flex;align-items:center;justify-content:center;font-size:1.3rem;">{icon}</div>
                    <div>
                        <div style="font-weight:700;font-size:0.92rem;color:#1a1a2e;">{name}</div>
                        <div style="font-size:0.75rem;color:#bbb;">{acc}</div>
                    </div>
                    <div style="margin-left:auto;">
                        <span style="background:#f0fff4;color:#22c55e;border-radius:20px;
                            padding:3px 10px;font-size:0.72rem;font-weight:600;">Active</span>
                    </div>
                </div>
                <div style="font-size:0.72rem;color:#bbb;">Available Balance</div>
                <div style="font-size:1.5rem;font-weight:800;color:#1a1a2e;">{balance}</div>
                <div class="progress-bar" style="margin-top:12px;">
                    <div class="progress-fill" style="width:65%;background:{color};"></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("**Recent Transactions**")
    txns = pd.DataFrame({
        "Date": ["Apr 14", "Apr 13", "Apr 12", "Apr 10", "Apr 8"],
        "Description": ["Figma Pro", "Client Payment - ABC Corp", "Adobe CC", "Freelance Project", "AWS Hosting"],
        "Category": ["Software", "Income", "Software", "Income", "Infrastructure"],
        "Amount": ["-$20", "+$2,500", "-$54", "+$800", "-$120"],
        "Status": ["✅", "✅", "✅", "✅", "✅"]
    })
    st.dataframe(txns, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────
# MEETINGS PAGE
# ─────────────────────────────────────────────
def meetings_page():
    st.markdown('<div class="section-title">Meetings</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Upcoming and past meetings</div>', unsafe_allow_html=True)

    col1, col2 = st.columns([1.6, 1])

    with col1:
        st.markdown("**Upcoming Meetings**")
        meetings = [
            ("📐", "Design Review", "Today, 10:00 AM", "Harry Joe, Sarah A.", "#FF6B2B", "In 30 min"),
            ("📊", "Analytics Sync", "Today, 2:00 PM", "Martha June, Tom B.", "#3D3B8E", "In 4h"),
            ("🎨", "Brand Workshop", "Tomorrow, 9:00 AM", "Full Team", "#22c55e", "Tomorrow"),
            ("📋", "Sprint Planning", "Apr 18, 11:00 AM", "Dev + Design", "#ec4899", "In 2d"),
            ("💼", "Client Call – ABC", "Apr 20, 3:00 PM", "Harry Joe", "#f59e0b", "In 4d"),
        ]
        for icon, title, time_str, attendees, color, badge in meetings:
            st.markdown(f"""
            <div style="background:white;border-radius:14px;padding:1rem 1.2rem;border:1px solid #f0f0f5;
                margin-bottom:10px;display:flex;align-items:center;gap:14px;">
                <div style="width:42px;height:42px;border-radius:12px;background:{color}18;
                    display:flex;align-items:center;justify-content:center;font-size:1.2rem;flex-shrink:0;">{icon}</div>
                <div style="flex:1;">
                    <div style="font-weight:700;font-size:0.88rem;color:#1a1a2e;">{title}</div>
                    <div style="font-size:0.75rem;color:#bbb;">{time_str} · {attendees}</div>
                </div>
                <span style="background:{color}18;color:{color};border-radius:20px;
                    padding:3px 10px;font-size:0.72rem;font-weight:600;white-space:nowrap;">{badge}</span>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        st.markdown("**Calendar — April 2026**")
        # Mini calendar
        days = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
        header = " | ".join(days)
        st.markdown(f"<div style='font-size:0.78rem;color:#bbb;font-weight:600;margin-bottom:8px;'>{header}</div>", unsafe_allow_html=True)

        weeks = [
            [" ", " ", 1, 2, 3, 4, 5],
            [6, 7, 8, 9, 10, 11, 12],
            [13, 14, 15, 16, 17, 18, 19],
            [20, 21, 22, 23, 24, 25, 26],
            [27, 28, 29, 30, " ", " ", " "],
        ]
        meeting_days = {16, 18, 20}
        today = 16
        cal_html = ""
        for week in weeks:
            row = ""
            for d in week:
                if d == " ":
                    row += "<span style='display:inline-block;width:28px;text-align:center;'>&nbsp;</span>"
                elif d == today:
                    row += f"<span style='display:inline-block;width:28px;text-align:center;background:#FF6B2B;color:white;border-radius:50%;font-weight:700;font-size:0.8rem;padding:2px 0;'>{d}</span>"
                elif d in meeting_days:
                    row += f"<span style='display:inline-block;width:28px;text-align:center;background:#3D3B8E18;color:#3D3B8E;border-radius:50%;font-weight:600;font-size:0.8rem;padding:2px 0;'>{d}</span>"
                else:
                    row += f"<span style='display:inline-block;width:28px;text-align:center;font-size:0.8rem;color:#444;padding:2px 0;'>{d}</span>"
            cal_html += f"<div style='margin-bottom:4px;'>{row}</div>"

        st.markdown(f"<div style='background:white;border-radius:14px;padding:1.2rem;border:1px solid #f0f0f5;'>{cal_html}</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div style="background:white;border-radius:14px;padding:1.2rem;border:1px solid #f0f0f5;">
            <div style="font-weight:700;font-size:0.88rem;color:#1a1a2e;margin-bottom:10px;">Meeting Stats</div>
            <div style="display:flex;justify-content:space-around;text-align:center;">
                <div><div style="font-size:1.4rem;font-weight:800;color:#FF6B2B;">5</div><div style="font-size:0.7rem;color:#bbb;">This Week</div></div>
                <div><div style="font-size:1.4rem;font-weight:800;color:#3D3B8E;">18</div><div style="font-size:0.7rem;color:#bbb;">This Month</div></div>
                <div><div style="font-size:1.4rem;font-weight:800;color:#22c55e;">94%</div><div style="font-size:0.7rem;color:#bbb;">Attended</div></div>
            </div>
        </div>
        """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# SETTINGS PAGE
# ─────────────────────────────────────────────
def settings_page():
    st.markdown('<div class="section-title">Settings</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Manage your preferences and account</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["👤 Profile", "🔔 Notifications", "🔒 Security"])

    with tab1:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown("""
            <div style="text-align:center;padding:1.5rem;background:white;border-radius:16px;
                border:1px solid #f0f0f5;margin-bottom:16px;">
                <div style="width:70px;height:70px;border-radius:50%;background:linear-gradient(135deg,#FF6B2B,#ff9a6b);
                    display:flex;align-items:center;justify-content:center;font-size:2rem;margin:0 auto 10px;">🕶️</div>
                <div style="font-weight:700;font-size:1rem;color:#1a1a2e;">AR Shakir</div>
                <div style="font-size:0.78rem;color:#bbb;">Sr. Visual Designer · Redwhale LLC</div>
            </div>
            """, unsafe_allow_html=True)
            st.button("📷 Change Avatar", use_container_width=True)

        with col2:
            st.text_input("Full Name", value="AR Shakir")
            st.text_input("Email", value="ar.shakir@redwhale.com")
            st.text_input("Role", value="Sr. Visual Designer")
            st.selectbox("Team", ["Redwhale LLC", "Design Studio", "Freelance"])
            st.button("💾 Save Changes", use_container_width=True)

    with tab2:
        st.markdown("**Email Notifications**")
        notifs = ["New message received", "Project updates", "Meeting reminders", "Invoice alerts", "Weekly summary"]
        for n in notifs:
            st.toggle(n, value=True)

        st.markdown("**Push Notifications**")
        push = ["Browser alerts", "Mobile push", "Desktop notifications"]
        for p in push:
            st.toggle(p, value=False)

    with tab3:
        st.text_input("Current Password", type="password")
        st.text_input("New Password", type="password")
        st.text_input("Confirm Password", type="password")
        st.button("🔒 Update Password", use_container_width=True)

        st.markdown("---")
        st.markdown("**Two-Factor Authentication**")
        st.toggle("Enable 2FA", value=True)
        st.markdown("**Active Sessions**")
        sessions = pd.DataFrame({
            "Device": ["MacBook Pro", "iPhone 15", "Chrome on Windows"],
            "Location": ["Algiers, DZ", "Algiers, DZ", "Paris, FR"],
            "Last Active": ["Now", "2h ago", "Yesterday"],
            "Status": ["✅ Current", "✅ Active", "⚠️ Unknown"]
        })
        st.dataframe(sessions, use_container_width=True, hide_index=True)