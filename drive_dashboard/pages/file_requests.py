import streamlit as st
from components.cards import topbar, section_header

def render():
    topbar("File Requests")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
        <div>
            <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">File Requests</h2>
            <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">Manage incoming and outgoing file requests</p>
        </div>
        <button style="background:#4F6EF7;color:white;border:none;border-radius:12px;
                       padding:10px 20px;font-size:14px;font-weight:600;cursor:pointer;
                       font-family:'Plus Jakarta Sans',sans-serif;">+ New Request</button>
    </div>
    """, unsafe_allow_html=True)

    requests = [
        {"title":"Design Assets for Q1 Campaign","from":"Marketing Team","due":"Dec 20, 2024","status":"Pending","color":"#F5A623","files":3,"progress":0},
        {"title":"Financial Statements 2024","from":"Finance Dept","due":"Dec 15, 2024","status":"In Progress","color":"#4F6EF7","files":5,"progress":60},
        {"title":"Product Photos — New Line","from":"Creative Dir.","due":"Dec 10, 2024","status":"Completed","color":"#2ECC71","files":12,"progress":100},
        {"title":"Legal Contracts Review","from":"Legal Team","due":"Jan 05, 2025","status":"Pending","color":"#F5A623","files":2,"progress":0},
        {"title":"User Research Data","from":"Research Team","due":"Dec 18, 2024","status":"In Progress","color":"#4F6EF7","files":8,"progress":35},
        {"title":"Press Kit Materials","from":"PR Agency","due":"Nov 30, 2024","status":"Overdue","color":"#F05454","files":6,"progress":45},
    ]

    for r in requests:
        status_bg = {
            "Pending": "rgba(245,166,35,0.12)",
            "In Progress": "rgba(79,110,247,0.12)",
            "Completed": "rgba(46,204,113,0.12)",
            "Overdue": "rgba(240,84,84,0.12)",
        }.get(r["status"], "rgba(139,146,165,0.12)")

        bar_width = r["progress"]
        st.markdown(f"""
        <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};margin-bottom:12px;">
            <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:12px;">
                <div>
                    <div style="color:{text};font-size:15px;font-weight:700;margin-bottom:4px;">{r['title']}</div>
                    <div style="color:{muted};font-size:13px;">From: <span style="color:#4F6EF7;font-weight:600;">{r['from']}</span></div>
                </div>
                <div style="display:flex;align-items:center;gap:12px;">
                    <span style="background:{status_bg};color:{r['color']};font-size:12px;
                                 font-weight:700;padding:4px 12px;border-radius:20px;">{r['status']}</span>
                    <span style="color:{muted};font-size:13px;">⋮</span>
                </div>
            </div>
            <div style="display:flex;align-items:center;gap:20px;margin-bottom:12px;">
                <div style="color:{muted};font-size:12px;">📅 Due: <span style="color:{text};font-weight:600;">{r['due']}</span></div>
                <div style="color:{muted};font-size:12px;">📁 <span style="color:{text};font-weight:600;">{r['files']} files expected</span></div>
                <div style="color:{muted};font-size:12px;">✅ <span style="color:{text};font-weight:600;">{r['progress']}% complete</span></div>
            </div>
            <div style="background:rgba(255,255,255,0.06);border-radius:6px;height:6px;overflow:hidden;">
                <div style="background:{r['color']};width:{bar_width}%;height:100%;border-radius:6px;
                             transition:width 0.5s ease;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
