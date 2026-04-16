import streamlit as st
from components.cards import topbar

def task_card(title, date_time, members, priority, color, tag):
    priority_colors = {"High":"#F05454","Medium":"#F5A623","Low":"#2ECC71"}
    pc = priority_colors.get(priority, "#8B92A5")
    avatars = "".join([f'<div style="width:26px;height:26px;background:{m[0]};border-radius:50%;border:2px solid var(--bg-card,#1A1D2E);display:flex;align-items:center;justify-content:center;color:white;font-size:10px;font-weight:700;margin-left:-6px;">{m[1]}</div>' for m in members])

    return f"""
    <div style="background:var(--tc-bg);border-radius:14px;padding:16px;margin-bottom:12px;
                border:1px solid var(--tc-border);cursor:pointer;transition:transform 0.2s;">
        <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:10px;">
            <span style="background:rgba({','.join(str(int(color.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.12);
                         color:{color};font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px;">{tag}</span>
            <span style="color:var(--tc-muted);font-size:16px;cursor:pointer;">⋮</span>
        </div>
        <div style="color:var(--tc-text);font-size:14px;font-weight:700;margin-bottom:8px;line-height:1.4;">{title}</div>
        <div style="color:var(--tc-muted);font-size:12px;margin-bottom:12px;">🕐 {date_time}</div>
        <div style="display:flex;align-items:center;justify-content:space-between;">
            <div style="display:flex;align-items:center;margin-left:6px;">{avatars}</div>
            <span style="background:rgba({','.join(str(int(pc.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.12);
                         color:{pc};font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px;">{priority}</span>
        </div>
    </div>
    """

def render():
    topbar("Task")
    dm = st.session_state.dark_mode
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    bg_card = "#1A1D2E" if dm else "#FFFFFF"
    bg_col = "#13162a" if dm else "#F5F6FA"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    # Inject CSS vars for task cards
    st.markdown(f"""
    <style>
    :root {{
        --tc-bg: {bg_card};
        --tc-border: {border};
        --tc-text: {text};
        --tc-muted: {muted};
    }}
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div style="padding: 24px 28px;">', unsafe_allow_html=True)

    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
        <div>
            <h2 style="color:{text};font-size:24px;font-weight:800;margin:0;">Task Board</h2>
            <p style="color:{muted};font-size:14px;margin:4px 0 0 0;">Track your project tasks and progress</p>
        </div>
        <button style="background:#4F6EF7;color:white;border:none;border-radius:12px;
                       padding:10px 20px;font-size:14px;font-weight:600;cursor:pointer;
                       font-family:'Plus Jakarta Sans',sans-serif;">+ Add Task</button>
    </div>
    """, unsafe_allow_html=True)

    # KPI row
    k1,k2,k3,k4 = st.columns(4, gap="medium")
    kpis = [("📋","Total Tasks","24","#4F6EF7"),("⏳","In Progress","8","#F5A623"),
            ("✅","Completed","12","#2ECC71"),("🔴","Overdue","4","#F05454")]
    for col,(icon,label,val,color) in zip([k1,k2,k3,k4],kpis):
        with col:
            st.markdown(f"""
            <div style="background:{bg_card};border-radius:18px;padding:18px;border:1px solid {border};text-align:center;margin-bottom:20px;">
                <div style="font-size:26px;margin-bottom:6px;">{icon}</div>
                <div style="color:{color};font-size:26px;font-weight:800;">{val}</div>
                <div style="color:{muted};font-size:12px;margin-top:2px;">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    # Kanban board
    cols = st.columns(3, gap="medium")
    members_a = [("#4F6EF7","J"),("#F05454","M"),("#2ECC71","K")]
    members_b = [("#F5A623","A"),("#9B59B6","B")]
    members_c = [("#F05454","C"),("#4F6EF7","D")]

    with cols[0]:
        st.markdown(f"""
        <div style="background:{bg_col};border-radius:18px;padding:16px;border:1px solid {border};">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="width:10px;height:10px;background:#F5A623;border-radius:50%;"></div>
                    <span style="color:{text};font-size:14px;font-weight:700;">To Do</span>
                    <span style="background:rgba(245,166,35,0.12);color:#F5A623;font-size:11px;
                                 font-weight:700;padding:2px 8px;border-radius:20px;">6</span>
                </div>
                <span style="color:{muted};cursor:pointer;">+ Add</span>
            </div>
        """, unsafe_allow_html=True)
        st.markdown(task_card("Review health care app UI","Dec 7, 2024 | 10:00 AM",members_a,"High","#F05454","Design"), unsafe_allow_html=True)
        st.markdown(task_card("Write Q4 report summary","Dec 9, 2024 | 2:00 PM",members_b,"Medium","#4F6EF7","Report"), unsafe_allow_html=True)
        st.markdown(task_card("Plan Q1 roadmap session","Dec 12, 2024 | 9:00 AM",members_c,"Low","#2ECC71","Planning"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with cols[1]:
        st.markdown(f"""
        <div style="background:{bg_col};border-radius:18px;padding:16px;border:1px solid {border};">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="width:10px;height:10px;background:#4F6EF7;border-radius:50%;"></div>
                    <span style="color:{text};font-size:14px;font-weight:700;">In Progress</span>
                    <span style="background:rgba(79,110,247,0.12);color:#4F6EF7;font-size:11px;
                                 font-weight:700;padding:2px 8px;border-radius:20px;">8</span>
                </div>
                <span style="color:{muted};cursor:pointer;">+ Add</span>
            </div>
        """, unsafe_allow_html=True)
        st.markdown(task_card("Design system update v2","Dec 8, 2024 | 11:00 AM",members_a,"High","#9B59B6","Design"), unsafe_allow_html=True)
        st.markdown(task_card("Client presentation deck","Dec 8, 2024 | 3:00 PM",members_c,"Medium","#F05454","Client"), unsafe_allow_html=True)
        st.markdown(task_card("Code review backend API","Dec 10, 2024 | 1:00 PM",members_b,"Medium","#4F6EF7","Dev"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with cols[2]:
        st.markdown(f"""
        <div style="background:{bg_col};border-radius:18px;padding:16px;border:1px solid {border};">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
                <div style="display:flex;align-items:center;gap:8px;">
                    <div style="width:10px;height:10px;background:#2ECC71;border-radius:50%;"></div>
                    <span style="color:{text};font-size:14px;font-weight:700;">Completed</span>
                    <span style="background:rgba(46,204,113,0.12);color:#2ECC71;font-size:11px;
                                 font-weight:700;padding:2px 8px;border-radius:20px;">12</span>
                </div>
                <span style="color:{muted};cursor:pointer;">+ Add</span>
            </div>
        """, unsafe_allow_html=True)
        st.markdown(task_card("Brand identity refresh","Dec 5, 2024 | 9:00 AM",members_a,"High","#F5A623","Design"), unsafe_allow_html=True)
        st.markdown(task_card("User research interviews","Dec 3, 2024 | 2:00 PM",members_b,"Low","#2ECC71","Research"), unsafe_allow_html=True)
        st.markdown(task_card("Fix login page bug","Dec 1, 2024 | 4:00 PM",members_c,"High","#F05454","Dev"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
