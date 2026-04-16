import streamlit as st
from components.cards import topbar, section_header, upload_button, folder_card, file_row
from components.calendar_widget import render_calendar, render_tasks
from components.storage_chart import render_storage_chart

def render():
    topbar("Dashboard")

    dm = st.session_state.dark_mode
    accent = "#4F6EF7"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    # Main layout: left content + right sidebar
    main_col, right_col = st.columns([2.6, 1], gap="medium")

    with main_col:
        st.markdown('<div style="padding: 24px 28px 0 28px;">', unsafe_allow_html=True)

        upload_button()

        # Welcome Banner
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#2D3561 0%,#1A1D2E 60%,#1e2040 100%);
                    border-radius:20px;padding:28px 32px;margin:20px 0;position:relative;overflow:hidden;
                    border:1px solid rgba(79,110,247,0.2);">
            <div style="position:absolute;right:0;top:0;bottom:0;width:280px;
                        background:linear-gradient(135deg,rgba(79,110,247,0.1),rgba(139,92,246,0.05));
                        display:flex;align-items:center;justify-content:center;font-size:80px;opacity:0.3;">🖥️</div>
            <div style="position:absolute;right:40px;top:50%;transform:translateY(-50%);font-size:100px;opacity:0.15;">👤</div>
            <div style="position:relative;z-index:1;max-width:60%;">
                <h2 style="color:white;font-size:26px;font-weight:800;margin:0 0 8px 0;line-height:1.2;">
                    Welcome Back Jannie
                </h2>
                <p style="color:rgba(255,255,255,0.65);font-size:14px;margin:0 0 20px 0;line-height:1.5;">
                    Get additional 500 GB space for your documents and files.<br>Unlock now for more space.
                </p>
                <button style="background:#4F6EF7;color:white;border:none;border-radius:10px;
                               padding:10px 22px;font-size:13px;font-weight:700;cursor:pointer;
                               font-family:'Plus Jakarta Sans',sans-serif;">Upgrade</button>
            </div>
            <div style="position:absolute;right:30px;top:20px;font-size:60px;opacity:0.6;">🌿</div>
        </div>
        """, unsafe_allow_html=True)

        # Folders
        section_header("Folders")
        f1, f2, f3 = st.columns(3, gap="medium")

        members1 = [{"color":"#F05454","initial":"J"},{"color":"#4F6EF7","initial":"M"},
                    {"color":"#2ECC71","initial":"K"},{"color":"#F5A623","initial":"A"},
                    {"color":"#9B59B6","initial":"B"}]
        members2 = [{"color":"#2ECC71","initial":"A"},{"color":"#F05454","initial":"S"},
                    {"color":"#4F6EF7","initial":"R"},{"color":"#F5A623","initial":"T"}]
        members3 = [{"color":"#9B59B6","initial":"C"},{"color":"#4F6EF7","initial":"D"},
                    {"color":"#2ECC71","initial":"E"},{"color":"#F05454","initial":"F"},
                    {"color":"#F5A623","initial":"G"}]

        with f1:
            st.markdown(folder_card("Design Shift","red","🎨","10","Dec 13, 2024",members1), unsafe_allow_html=True)
        with f2:
            st.markdown(folder_card("Health Care App","blue","💊","12","Nov 04, 2024",members2), unsafe_allow_html=True)
        with f3:
            st.markdown(folder_card("Food truck Website","yellow","🍔","16","Nov 05, 2024",members3), unsafe_allow_html=True)

        # Recent Files
        section_header("Recent Files")

        st.markdown(
            file_row("Design Thinking Process","PDF","Only You","Dec 13, 2024","2 MB"),
            unsafe_allow_html=True
        )
        st.markdown(
            file_row("Design Thinking Process","PNG","10 Members","Nov 04, 2024","10 MB", highlight=True),
            unsafe_allow_html=True
        )
        st.markdown(
            file_row("Characters Animation","ZIP","15 Members","Nov 01, 2024","50 MB"),
            unsafe_allow_html=True
        )

        st.markdown('</div>', unsafe_allow_html=True)

    with right_col:
        st.markdown('<div style="padding: 24px 24px 0 0;">', unsafe_allow_html=True)
        render_calendar()
        render_tasks()
        render_storage_chart()
        st.markdown('</div>', unsafe_allow_html=True)
