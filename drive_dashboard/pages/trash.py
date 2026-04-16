import streamlit as st
from components.cards import topbar, section_header

def render():
    topbar("Trash")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
        <div>
            <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">Trash</h2>
            <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">Files are deleted permanently after 30 days</p>
        </div>
        <div style="display:flex;gap:10px;">
            <button style="background:rgba(240,84,84,0.1);color:#F05454;border:1px solid rgba(240,84,84,0.3);
                           border-radius:12px;padding:10px 20px;font-size:14px;font-weight:600;cursor:pointer;
                           font-family:'Plus Jakarta Sans',sans-serif;">🗑️ Empty Trash</button>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Info banner
    st.markdown(f"""
    <div style="background:rgba(245,166,35,0.08);border:1px solid rgba(245,166,35,0.2);
                border-radius:14px;padding:14px 20px;margin-bottom:24px;
                display:flex;align-items:center;gap:12px;">
        <span style="font-size:20px;">⚠️</span>
        <span style="color:#F5A623;font-size:14px;font-weight:500;">
            Items in trash will be permanently deleted after <strong>30 days</strong>. Restore them before they're gone.
        </span>
    </div>
    """, unsafe_allow_html=True)

    trash_items = [
        {"name":"Old Logo Files","type":"ZIP","size":"45 MB","deleted":"Dec 08, 2024","days_left":22,"icon":"📦"},
        {"name":"Draft Presentation","type":"PDF","size":"12 MB","deleted":"Dec 05, 2024","days_left":19,"icon":"📄"},
        {"name":"Backup Images","type":"PNG","size":"230 MB","deleted":"Nov 28, 2024","days_left":12,"icon":"🖼️"},
        {"name":"Old Contracts","type":"DOC","size":"3.4 MB","deleted":"Nov 20, 2024","days_left":4,"icon":"📋"},
        {"name":"Legacy Codebase","type":"ZIP","size":"158 MB","deleted":"Nov 15, 2024","days_left":0,"icon":"💻"},
    ]

    for item in trash_items:
        urgency = "#F05454" if item["days_left"] <= 5 else ("#F5A623" if item["days_left"] <= 15 else muted)
        days_text = "Deletes today!" if item["days_left"] == 0 else f"{item['days_left']} days left"

        type_colors = {"ZIP":"#2ECC71","PDF":"#F05454","PNG":"#4F6EF7","DOC":"#F5A623"}
        tc = type_colors.get(item["type"], "#8B92A5")

        st.markdown(f"""
        <div style="background:{bg_card};border-radius:18px;padding:18px 20px;
                    border:1px solid {border};margin-bottom:12px;
                    display:flex;align-items:center;gap:16px;">
            <div style="width:44px;height:44px;background:rgba(240,84,84,0.08);border-radius:12px;
                        display:flex;align-items:center;justify-content:center;font-size:22px;flex-shrink:0;">
                {item['icon']}
            </div>
            <div style="flex:1;">
                <div style="color:{text};font-size:14px;font-weight:700;">{item['name']}</div>
                <div style="color:{muted};font-size:12px;margin-top:2px;">Deleted {item['deleted']} · {item['size']}</div>
            </div>
            <div style="background:rgba({','.join(str(int(tc.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.1);
                        color:{tc};font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px;width:40px;text-align:center;">
                {item['type']}
            </div>
            <div style="color:{urgency};font-size:12px;font-weight:600;width:110px;text-align:center;">⏱ {days_text}</div>
            <div style="display:flex;gap:10px;">
                <button style="background:rgba(79,110,247,0.1);color:#4F6EF7;border:1px solid rgba(79,110,247,0.2);
                               border-radius:8px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;
                               font-family:'Plus Jakarta Sans',sans-serif;">↩ Restore</button>
                <button style="background:rgba(240,84,84,0.1);color:#F05454;border:1px solid rgba(240,84,84,0.2);
                               border-radius:8px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;
                               font-family:'Plus Jakarta Sans',sans-serif;">✕ Delete</button>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
