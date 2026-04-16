import streamlit as st
from components.cards import topbar, section_header, folder_card, file_row

def render():
    topbar("Starred")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="margin-bottom:24px;">
        <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">Starred</h2>
        <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">Your favourite files and folders</p>
    </div>
    """, unsafe_allow_html=True)

    # Starred folders
    section_header("Starred Folders")
    c1, c2, c3 = st.columns(3, gap="medium")
    members = [{"color":"#F05454","initial":"J"},{"color":"#4F6EF7","initial":"M"},{"color":"#2ECC71","initial":"K"}]
    with c1:
        st.markdown(folder_card("Design Shift","red","🎨","10","Dec 13, 2024",members), unsafe_allow_html=True)
    with c2:
        st.markdown(folder_card("Brand Assets","purple","🎯","8","Oct 12, 2024",members), unsafe_allow_html=True)
    with c3:
        st.markdown(folder_card("Marketing Kit","green","📣","14","Sep 20, 2024",members), unsafe_allow_html=True)

    section_header("Starred Files")
    files = [
        ("Annual Report 2024","PDF","Only You","Dec 13, 2024","4.2 MB",False),
        ("UI Design System","PNG","10 Members","Nov 04, 2024","45 MB",True),
        ("Brand Guidelines","PDF","Design Team","Oct 20, 2024","12 MB",False),
        ("Product Roadmap Q1","DOC","Management","Oct 05, 2024","2.1 MB",False),
    ]
    for f in files:
        st.markdown(file_row(*f), unsafe_allow_html=True)

    # Quick access
    st.markdown(f"""
    <div style="margin-top:24px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
            <span style="color:{text};font-size:18px;font-weight:700;">Recently Starred</span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;">
    """, unsafe_allow_html=True)

    quick = [
        ("🗒️","Meeting Notes","DOC","2h ago","#4F6EF7"),
        ("📊","Q3 Analytics","XLS","5h ago","#2ECC71"),
        ("🎨","Mockup v3","PNG","Yesterday","#F05454"),
        ("📋","Brief 2025","PDF","2 days ago","#F5A623"),
    ]
    cols = st.columns(4, gap="medium")
    for col, (icon, name, ftype, when, color) in zip(cols, quick):
        with col:
            st.markdown(f"""
            <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};
                        text-align:center;cursor:pointer;transition:transform 0.2s;">
                <div style="font-size:36px;margin-bottom:12px;">{icon}</div>
                <div style="color:{text};font-size:14px;font-weight:700;margin-bottom:4px;">{name}</div>
                <div style="background:rgba({','.join(str(int(color.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.12);
                            color:{color};font-size:11px;font-weight:700;padding:3px 10px;
                            border-radius:20px;display:inline-block;margin-bottom:8px;">{ftype}</div>
                <div style="color:{muted};font-size:12px;">⭐ {when}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('</div></div></div>', unsafe_allow_html=True)
