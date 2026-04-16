import streamlit as st
from components.cards import topbar, section_header, file_row

def render():
    topbar("Shared Files")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
        <div>
            <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">Shared Files</h2>
            <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">Files shared with you and by you</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Filter tabs
    st.markdown(f"""
    <div style="display:flex;gap:4px;background:{bg_card};border-radius:14px;padding:6px;
                border:1px solid {border};width:fit-content;margin-bottom:24px;">
        <button style="background:#4F6EF7;color:white;border:none;border-radius:10px;
                       padding:8px 20px;font-size:13px;font-weight:600;cursor:pointer;
                       font-family:'Plus Jakarta Sans',sans-serif;">All Files</button>
        <button style="background:transparent;color:{muted};border:none;border-radius:10px;
                       padding:8px 20px;font-size:13px;font-weight:500;cursor:pointer;
                       font-family:'Plus Jakarta Sans',sans-serif;">Shared With Me</button>
        <button style="background:transparent;color:{muted};border:none;border-radius:10px;
                       padding:8px 20px;font-size:13px;font-weight:500;cursor:pointer;
                       font-family:'Plus Jakarta Sans',sans-serif;">Shared By Me</button>
    </div>
    """, unsafe_allow_html=True)

    # Team Members sharing stats
    col1, col2, col3, col4 = st.columns(4, gap="medium")
    stats = [
        ("📄", "Total Shared", "142", "#4F6EF7"),
        ("👥", "Shared With Me", "89", "#2ECC71"),
        ("📤", "Shared By Me", "53", "#F5A623"),
        ("⭐", "Starred Shared", "24", "#F05454"),
    ]
    for col, (icon, label, val, color) in zip([col1,col2,col3,col4], stats):
        with col:
            st.markdown(f"""
            <div style="background:{bg_card};border-radius:18px;padding:20px;
                        border:1px solid {border};text-align:center;">
                <div style="font-size:32px;margin-bottom:8px;">{icon}</div>
                <div style="color:{color};font-size:28px;font-weight:800;">{val}</div>
                <div style="color:{muted};font-size:13px;margin-top:4px;">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    section_header("Shared With Me")
    files = [
        ("Q4 Financial Report","PDF","Sarah Connor","Dec 10, 2024","8.5 MB",False),
        ("Brand Guidelines v2","PNG","Design Team","Nov 28, 2024","32 MB",True),
        ("Developer Docs","ZIP","Tech Team","Nov 15, 2024","15 MB",False),
        ("Meeting Transcript","DOC","Mark Johnson","Nov 10, 2024","0.8 MB",False),
    ]
    for f in files:
        st.markdown(file_row(*f), unsafe_allow_html=True)

    section_header("Shared By Me")
    files2 = [
        ("Design Mockups","PNG","UX Team","Dec 12, 2024","22 MB",False),
        ("Project Proposal","PDF","Client","Dec 01, 2024","3.2 MB",True),
        ("Source Code","ZIP","Dev Team","Nov 20, 2024","80 MB",False),
    ]
    for f in files2:
        st.markdown(file_row(*f), unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
