import streamlit as st
import plotly.graph_objects as go

def render_storage_chart():
    dm = st.session_state.dark_mode
    bg = "#1A1D2E" if dm else "#FFFFFF"
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"
    grid_color = "rgba(255,255,255,0.05)" if dm else "rgba(0,0,0,0.05)"

    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug"]
    uploads =       [32,25,38,30,45,28,42,35]
    files_received = [18,22,15,25,20,30,18,22]
    space_left =    [10,13,8,15,12,18,10,13]

    fig = go.Figure()

    fig.add_trace(go.Bar(name="Uploads", x=months, y=uploads,
                         marker_color="#F05454", marker_line_width=0,
                         width=0.25, offsetgroup=0))
    fig.add_trace(go.Bar(name="Files Received", x=months, y=files_received,
                         marker_color="#F5A623", marker_line_width=0,
                         width=0.25, offsetgroup=1))
    fig.add_trace(go.Bar(name="Space Left", x=months, y=space_left,
                         marker_color="#4F6EF7", marker_line_width=0,
                         width=0.25, offsetgroup=2))

    fig.update_layout(
        barmode='group',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Plus Jakarta Sans', color=muted, size=11),
        height=200,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.35,
            xanchor="center", x=0.5,
            font=dict(size=10, color=muted),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(
            gridcolor=grid_color, showgrid=False,
            tickfont=dict(color=muted, size=10),
            linecolor="rgba(0,0,0,0)",
        ),
        yaxis=dict(
            gridcolor=grid_color,
            tickfont=dict(color=muted, size=10),
            linecolor="rgba(0,0,0,0)",
            range=[0, 55],
            dtick=10,
        ),
        bargap=0.15,
        bargroupgap=0.05,
    )

    st.markdown(f"""
    <div style="background:{bg};border-radius:18px;padding:20px;border:1px solid {border};margin-top:16px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
            <div>
                <span style="color:{text};font-size:16px;font-weight:700;">Storage</span>
                <span style="color:#4F6EF7;font-size:12px;font-weight:700;margin-left:8px;">Available Space 73 GB</span>
            </div>
            <div style="display:flex;align-items:center;gap:6px;">
                <span style="color:{text};font-size:13px;font-weight:600;cursor:pointer;">📅 Month ▾</span>
                <span style="color:#4F6EF7;font-size:13px;font-weight:600;cursor:pointer;">View All ›</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.container():
        col_inner, = st.columns([1])
        st.markdown(f"""
        <div style="background:{bg};border-radius:0 0 18px 18px;padding:0 20px 16px 20px;
                    border:1px solid {border};border-top:none;margin-top:-20px;">
        """, unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)

def render_storage_full():
    """Full page storage/statistics chart"""
    dm = st.session_state.dark_mode
    bg = "#1A1D2E" if dm else "#FFFFFF"
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"
    grid_color = "rgba(255,255,255,0.05)" if dm else "rgba(0,0,0,0.05)"

    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    uploads       = [32,25,38,30,45,28,42,35,40,38,44,50]
    files_received= [18,22,15,25,20,30,18,22,26,20,28,32]
    space_left    = [10,13,8,15,12,18,10,13,15,12,16,18]

    fig = go.Figure()
    fig.add_trace(go.Bar(name="Uploads", x=months, y=uploads,
                         marker_color="#F05454", marker_line_width=0, width=0.25, offsetgroup=0))
    fig.add_trace(go.Bar(name="Files Received", x=months, y=files_received,
                         marker_color="#F5A623", marker_line_width=0, width=0.25, offsetgroup=1))
    fig.add_trace(go.Bar(name="Space Left", x=months, y=space_left,
                         marker_color="#4F6EF7", marker_line_width=0, width=0.25, offsetgroup=2))

    fig.update_layout(
        barmode='group',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Plus Jakarta Sans', color=muted, size=11),
        height=320,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.2,
            xanchor="center", x=0.5,
            font=dict(size=11, color=muted),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(gridcolor=grid_color, showgrid=False, tickfont=dict(color=muted, size=11)),
        yaxis=dict(gridcolor=grid_color, tickfont=dict(color=muted, size=11), range=[0, 60], dtick=10),
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
