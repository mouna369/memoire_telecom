import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from components.cards import topbar
from components.storage_chart import render_storage_full

def render():
    topbar("Statistics")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"
    grid = "rgba(255,255,255,0.05)" if dm else "rgba(0,0,0,0.05)"

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="margin-bottom:24px;">
        <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">Statistics</h2>
        <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">Overview of your storage and file activity</p>
    </div>
    """, unsafe_allow_html=True)

    # KPI cards
    c1, c2, c3, c4 = st.columns(4, gap="medium")
    kpis = [
        ("☁️","Total Storage","100 GB","Used 27%","#4F6EF7"),
        ("📁","Total Files","1,248","+42 this month","#2ECC71"),
        ("📤","Uploads","324","This month","#F5A623"),
        ("👥","Collaborators","18","Active users","#F05454"),
    ]
    for col, (icon, label, val, sub, color) in zip([c1,c2,c3,c4], kpis):
        with col:
            st.markdown(f"""
            <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};">
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
                    <span style="font-size:24px;">{icon}</span>
                    <span style="background:rgba({','.join(str(int(color.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.12);
                                 color:{color};font-size:10px;font-weight:700;padding:3px 8px;border-radius:20px;">LIVE</span>
                </div>
                <div style="color:{text};font-size:28px;font-weight:800;margin-bottom:2px;">{val}</div>
                <div style="color:{muted};font-size:13px;margin-bottom:4px;">{label}</div>
                <div style="color:{color};font-size:11px;font-weight:600;">{sub}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Two columns: bar chart + donut
    left, right = st.columns([2, 1], gap="medium")

    with left:
        st.markdown(f"""
        <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
                <span style="color:{text};font-size:16px;font-weight:700;">Storage Activity — Full Year</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown(f'<div style="background:{bg_card};border-radius:0 0 18px 18px;padding:0 20px 20px 20px;border:1px solid {border};border-top:none;margin-top:-16px;">', unsafe_allow_html=True)
        render_storage_full()
        st.markdown('</div>', unsafe_allow_html=True)

    with right:
        # Donut chart
        labels = ["Documents","Images","Videos","Audio","Other"]
        values = [35, 28, 20, 10, 7]
        colors = ["#4F6EF7","#F05454","#F5A623","#2ECC71","#9B59B6"]

        fig = go.Figure(data=[go.Pie(
            labels=labels, values=values,
            hole=0.65,
            marker=dict(colors=colors, line=dict(width=0)),
            textinfo='none',
            hovertemplate='<b>%{label}</b><br>%{value}%<extra></extra>',
        )])
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            height=280, margin=dict(l=0,r=0,t=10,b=0),
            showlegend=False,
            annotations=[dict(text='73 GB<br><span style="font-size:10px">Free</span>',
                             font_size=14, showarrow=False, font_color=text)]
        )

        legend_html = "".join([f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;"><div style="width:10px;height:10px;background:{c};border-radius:50%;flex-shrink:0;"></div><span style="color:{muted};font-size:12px;flex:1;">{l}</span><span style="color:{text};font-size:12px;font-weight:700;">{v}%</span></div>' for l,v,c in zip(labels,values,colors)])

        st.markdown(f"""
        <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};">
            <span style="color:{text};font-size:16px;font-weight:700;">Storage Breakdown</span>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown(f'<div style="padding:0 4px;">{legend_html}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Line chart — upload trends
    st.markdown(f"""
    <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};">
        <span style="color:{text};font-size:16px;font-weight:700;">Upload Trends — Last 30 Days</span>
    </div>
    <div style="background:{bg_card};border-radius:0 0 18px 18px;padding:0 20px 20px;border:1px solid {border};border-top:none;margin-top:-16px;">
    """, unsafe_allow_html=True)

    days = list(range(1, 31))
    uploads = [5,8,3,12,9,15,7,10,14,6,11,18,9,13,7,16,12,8,20,14,9,17,11,6,15,13,10,18,14,12]

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=days, y=uploads,
        fill='tozeroy',
        fillcolor='rgba(79,110,247,0.1)',
        line=dict(color='#4F6EF7', width=2.5, shape='spline'),
        mode='lines',
        name='Uploads',
        hovertemplate='Day %{x}: <b>%{y} uploads</b><extra></extra>',
    ))
    fig2.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        height=220, margin=dict(l=0,r=0,t=10,b=0),
        showlegend=False,
        xaxis=dict(gridcolor=grid, showgrid=False, tickfont=dict(color=muted, size=10), tickmode='linear', tick0=1, dtick=5),
        yaxis=dict(gridcolor=grid, tickfont=dict(color=muted, size=10), range=[0, 25]),
    )
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
