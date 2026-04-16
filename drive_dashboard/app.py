import streamlit as st

st.set_page_config(
    page_title="Drive Dashboard",
    page_icon="💠",
    layout="wide",
    initial_sidebar_state="expanded"
)

from styles.theme import inject_theme
from components.sidebar import render_sidebar

# Init session state
if "page" not in st.session_state:
    st.session_state.page = "dashboard"
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True

inject_theme(st.session_state.dark_mode)
render_sidebar()

# Route to pages
page = st.session_state.page

if page == "dashboard":
    from pages.dashboard import render
    render()
elif page == "my_drive":
    from pages.my_drive import render
    render()
elif page == "shared_files":
    from pages.shared_files import render
    render()
elif page == "file_requests":
    from pages.file_requests import render
    render()
elif page == "starred":
    from pages.starred import render
    render()
elif page == "trash":
    from pages.trash import render
    render()
elif page == "statistics":
    from pages.statistics import render
    render()
elif page == "task":
    from pages.task import render
    render()
