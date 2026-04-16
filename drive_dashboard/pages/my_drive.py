import streamlit as st
from components.cards import topbar, section_header, folder_card, file_row

def render():
    topbar("My Drive")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    # Header
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
        <div>
            <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">My Drive</h2>
            <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">All your personal files and folders</p>
        </div>
        <div style="display:flex;gap:10px;">
            <button style="background:#4F6EF7;color:white;border:none;border-radius:12px;
                           padding:10px 20px;font-size:14px;font-weight:600;cursor:pointer;
                           font-family:'Plus Jakarta Sans',sans-serif;">☁️ Upload File</button>
            <button style="background:rgba(79,110,247,0.1);color:#4F6EF7;border:1px solid rgba(79,110,247,0.3);
                           border-radius:12px;padding:10px 20px;font-size:14px;font-weight:600;cursor:pointer;
                           font-family:'Plus Jakarta Sans',sans-serif;">📁 New Folder</button>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Storage bar
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"
    st.markdown(f"""
    <div style="background:{bg_card};border-radius:18px;padding:20px;border:1px solid {border};margin-bottom:24px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
            <span style="color:{text};font-size:15px;font-weight:600;">Storage Used</span>
            <span style="color:#4F6EF7;font-size:15px;font-weight:700;">27 / 100 GB</span>
        </div>
        <div style="background:rgba(255,255,255,0.06);border-radius:8px;height:10px;overflow:hidden;">
            <div style="background:linear-gradient(90deg,#4F6EF7,#8B5CF6);width:27%;height:100%;border-radius:8px;"></div>
        </div>
        <div style="display:flex;gap:24px;margin-top:16px;">
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="width:10px;height:10px;background:#4F6EF7;border-radius:50%;"></div>
                <span style="color:{muted};font-size:12px;">Documents — 12 GB</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="width:10px;height:10px;background:#F05454;border-radius:50%;"></div>
                <span style="color:{muted};font-size:12px;">Images — 8 GB</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="width:10px;height:10px;background:#F5A623;border-radius:50%;"></div>
                <span style="color:{muted};font-size:12px;">Videos — 4 GB</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="width:10px;height:10px;background:#2ECC71;border-radius:50%;"></div>
                <span style="color:{muted};font-size:12px;">Other — 3 GB</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    section_header("Folders")
    c1, c2, c3, c4 = st.columns(4, gap="medium")
    members = [{"color":"#F05454","initial":"J"},{"color":"#4F6EF7","initial":"M"}]
    with c1:
        st.markdown(folder_card("Design Shift","red","🎨","10","Dec 13, 2024",members), unsafe_allow_html=True)
    with c2:
        st.markdown(folder_card("Health Care App","blue","💊","12","Nov 04, 2024",members), unsafe_allow_html=True)
    with c3:
        st.markdown(folder_card("Food Truck Website","yellow","🍔","16","Nov 05, 2024",members), unsafe_allow_html=True)
    with c4:
        st.markdown(folder_card("Brand Assets","purple","🎯","8","Oct 12, 2024",members), unsafe_allow_html=True)

    section_header("All Files")
    files = [
        ("Annual Report 2024","PDF","Only You","Dec 13, 2024","4.2 MB",False),
        ("UI Design System","PNG","10 Members","Nov 04, 2024","45 MB",True),
        ("Project Assets","ZIP","15 Members","Nov 01, 2024","120 MB",False),
        ("Meeting Notes","DOC","5 Members","Oct 28, 2024","1.1 MB",False),
        ("Budget Forecast","XLS","Only You","Oct 15, 2024","2.3 MB",False),
        ("Product Demo","MP4","8 Members","Oct 10, 2024","250 MB",False),
    ]
    for f in files:
        st.markdown(file_row(*f), unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
