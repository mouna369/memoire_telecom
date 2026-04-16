import streamlit as st

def nav_item(icon, label, page_key):
    is_active = st.session_state.page == page_key
    active_style = """
        background: rgba(79,110,247,0.15);
        color: #4F6EF7;
        border-left: 3px solid #4F6EF7;
    """ if is_active else ""

    clicked = st.sidebar.button(
        f"{icon}  {label}",
        key=f"nav_{page_key}",
        use_container_width=True,
    )
    if clicked:
        st.session_state.page = page_key
        st.rerun()

    if is_active:
        st.sidebar.markdown(f"""
        <style>
        div[data-testid="stButton"] button[kind="secondary"][id*="nav_{page_key}"] {{
            background: rgba(79,110,247,0.15) !important;
            color: #4F6EF7 !important;
        }}
        </style>
        """, unsafe_allow_html=True)

def render_sidebar():
    with st.sidebar:
        # Logo
        st.markdown("""
        <div style="padding: 28px 20px 20px 20px; display:flex; align-items:center; gap:10px;">
            <div style="width:36px;height:36px;background:linear-gradient(135deg,#4F6EF7,#8B5CF6);
                        border-radius:10px;display:flex;align-items:center;justify-content:center;
                        font-size:18px;">💠</div>
            <span style="color:#FFFFFF;font-size:22px;font-weight:800;letter-spacing:-0.5px;">Drive.</span>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        nav_item("☁️", "My Drive", "my_drive")
        nav_item("📤", "Shared Files", "shared_files")
        nav_item("📋", "File Requests", "file_requests")
        nav_item("⭐", "Starred", "starred")
        nav_item("🗑️", "Trash", "trash")

        st.markdown("---")

        nav_item("📊", "Statistics", "statistics")
        nav_item("✅", "Task", "task")

        st.markdown("<div style='flex:1'></div>", unsafe_allow_html=True)
        st.markdown("<br>" * 4, unsafe_allow_html=True)

        # Storage indicator
        st.markdown("""
        <div style="padding: 16px 20px; border-top: 1px solid rgba(255,255,255,0.06);">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
                <span style="font-size:16px;">☁️</span>
                <span style="color:#8B92A5;font-size:12px;font-weight:500;">Storage</span>
                <span style="color:#4F6EF7;font-size:12px;font-weight:700;margin-left:auto;">27%</span>
            </div>
            <div style="background:rgba(255,255,255,0.08);border-radius:6px;height:5px;">
                <div style="background:linear-gradient(90deg,#4F6EF7,#8B5CF6);width:27%;height:100%;border-radius:6px;"></div>
            </div>
            <div style="color:#5C6178;font-size:11px;margin-top:6px;">27/100 GB Used</div>
        </div>
        """, unsafe_allow_html=True)

        # Dark/Light toggle
        st.markdown("<div style='padding: 0 20px 16px 20px;'>", unsafe_allow_html=True)
        dark = st.toggle("🌙 Dark Mode", value=st.session_state.dark_mode, key="theme_toggle")
        if dark != st.session_state.dark_mode:
            st.session_state.dark_mode = dark
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
