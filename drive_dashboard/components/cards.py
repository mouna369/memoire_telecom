import streamlit as st

def card(content_html, padding="20px", extra_style=""):
    bg = "var(--bg-card)"
    border = "var(--border)"
    shadow = "var(--shadow)"
    st.markdown(f"""
    <div style="background:{bg};border:1px solid {border};border-radius:18px;
                padding:{padding};box-shadow:{shadow};{extra_style}">
        {content_html}
    </div>
    """, unsafe_allow_html=True)

def topbar(page_title: str):
    dm = st.session_state.dark_mode
    bg = "#1A1D2E" if dm else "#FFFFFF"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"
    text = "#FFFFFF" if dm else "#1A1D2E"
    input_bg = "#252A3D" if dm else "#F5F6FA"
    input_text = "#8B92A5"

    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;
                background:{bg};padding:16px 28px;border-bottom:1px solid {border};
                position:sticky;top:0;z-index:100;">
        <div style="display:flex;align-items:center;gap:12px;flex:1;">
            <div style="background:{input_bg};border-radius:12px;padding:10px 16px;
                        display:flex;align-items:center;gap:10px;width:320px;">
                <span style="color:{input_text};font-size:15px;">🔍</span>
                <span style="color:{input_text};font-size:14px;">Search files, folders...</span>
            </div>
        </div>
        <div style="display:flex;align-items:center;gap:20px;">
            <span style="color:{input_text};font-size:18px;cursor:pointer;">❓</span>
            <span style="color:{input_text};font-size:18px;cursor:pointer;">⚙️</span>
            <span style="color:{input_text};font-size:18px;cursor:pointer;">🔔</span>
            <div style="display:flex;align-items:center;gap:8px;cursor:pointer;">
                <div style="width:36px;height:36px;background:linear-gradient(135deg,#F05454,#F5A623);
                            border-radius:50%;display:flex;align-items:center;justify-content:center;
                            color:white;font-weight:700;font-size:14px;">J</div>
                <span style="color:{text};font-size:14px;font-weight:600;">Jannie ▾</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def section_header(title, action="View All"):
    text = "var(--text-primary)"
    muted = "var(--text-muted)"
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin:24px 0 14px 0;">
        <span style="color:{text};font-size:18px;font-weight:700;">{title}</span>
        <span style="color:#4F6EF7;font-size:13px;font-weight:600;cursor:pointer;">{action} ›</span>
    </div>
    """, unsafe_allow_html=True)

def upload_button():
    st.markdown("""
    <div style="display:flex;align-items:center;gap:24px;margin-bottom:4px;">
        <div style="display:flex;align-items:center;gap:12px;">
            <span style="color:var(--text-primary);font-size:22px;font-weight:800;">Dashboard</span>
            <div style="display:flex;gap:8px;">
                <button style="background:var(--hover-bg);border:none;border-radius:8px;
                               padding:6px 10px;cursor:pointer;font-size:16px;color:var(--text-secondary);">⊞</button>
                <button style="background:var(--hover-bg);border:none;border-radius:8px;
                               padding:6px 10px;cursor:pointer;font-size:16px;color:var(--text-secondary);">☰</button>
            </div>
        </div>
        <div style="margin-left:auto;">
            <button style="background:linear-gradient(135deg,#4F6EF7,#8B5CF6);color:white;border:none;
                           border-radius:12px;padding:10px 20px;font-size:14px;font-weight:600;
                           cursor:pointer;display:flex;align-items:center;gap:8px;font-family:'Plus Jakarta Sans',sans-serif;">
                ☁️ Upload File
            </button>
        </div>
    </div>
    """, unsafe_allow_html=True)

def folder_card(name, color, icon, files, created, members=None):
    colors = {
        "red":    ("#F05454", "rgba(240,84,84,0.15)"),
        "blue":   ("#4F6EF7", "rgba(79,110,247,0.15)"),
        "yellow": ("#F5A623", "rgba(245,166,35,0.15)"),
        "green":  ("#2ECC71", "rgba(46,204,113,0.15)"),
        "purple": ("#9B59B6", "rgba(155,89,182,0.15)"),
    }
    c, bg = colors.get(color, colors["blue"])
    member_html = ""
    if members:
        avatars = ""
        for m in members[:3]:
            avatars += f'<div style="width:24px;height:24px;background:{m["color"]};border-radius:50%;border:2px solid {bg};display:flex;align-items:center;justify-content:center;color:white;font-size:10px;font-weight:700;margin-left:-6px;">{m["initial"]}</div>'
        extra = f'+{len(members)-3}' if len(members) > 3 else ""
        member_html = f'<div style="display:flex;align-items:center;margin-left:6px;">{avatars}</div>'
        if extra:
            member_html += f'<span style="color:{c};font-size:11px;font-weight:700;margin-left:8px;">{extra}</span>'

    return f"""
    <div style="background:{c};border-radius:18px;padding:20px;position:relative;cursor:pointer;
                transition:transform 0.2s;min-height:160px;overflow:hidden;">
        <div style="position:absolute;top:16px;left:16px;font-size:24px;">{icon}</div>
        <div style="position:absolute;top:16px;right:12px;color:rgba(255,255,255,0.7);font-size:16px;cursor:pointer;">⋮</div>
        <div style="margin-top:60px;">
            <div style="color:white;font-size:16px;font-weight:700;margin-bottom:8px;">{name}</div>
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
                {member_html}
            </div>
            <div style="color:rgba(255,255,255,0.75);font-size:12px;display:flex;align-items:center;gap:4px;">
                📁 {files} Files
            </div>
            <div style="color:rgba(255,255,255,0.6);font-size:11px;margin-top:4px;">
                🕐 Created on {created}
            </div>
        </div>
    </div>
    """

def file_row(name, file_type, shared_with, date, size, highlight=False):
    type_colors = {
        "PDF": ("#F05454", "rgba(240,84,84,0.12)"),
        "PNG": ("#4F6EF7", "rgba(79,110,247,0.12)"),
        "ZIP": ("#2ECC71", "rgba(46,204,113,0.12)"),
        "DOC": ("#4F6EF7", "rgba(79,110,247,0.12)"),
        "XLS": ("#2ECC71", "rgba(46,204,113,0.12)"),
        "MP4": ("#F5A623", "rgba(245,166,35,0.12)"),
    }
    c, bg = type_colors.get(file_type, ("#8B92A5", "rgba(139,146,165,0.12)"))
    row_bg = "rgba(79,110,247,0.08)" if highlight else "var(--bg-card)"
    name_color = "white" if highlight else "var(--text-primary)"
    meta_color = "rgba(255,255,255,0.7)" if highlight else "var(--text-secondary)"

    return f"""
    <div style="display:flex;align-items:center;gap:16px;background:{row_bg};
                border-radius:14px;padding:14px 18px;margin-bottom:10px;
                border:1px solid {'rgba(79,110,247,0.2)' if highlight else 'var(--border)'};
                transition:all 0.2s;cursor:pointer;">
        <div style="width:40px;height:40px;background:{bg};border-radius:10px;
                    display:flex;align-items:center;justify-content:center;
                    color:{c};font-size:11px;font-weight:800;flex-shrink:0;">{file_type}</div>
        <div style="flex:1;min-width:0;">
            <div style="color:{name_color};font-size:14px;font-weight:600;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{name}</div>
        </div>
        <div style="color:{meta_color};font-size:12px;width:100px;text-align:center;">{shared_with}</div>
        <div style="color:{meta_color};font-size:12px;width:90px;text-align:center;">{date}</div>
        <div style="color:{meta_color};font-size:12px;width:60px;text-align:right;">{size}</div>
        <div style="display:flex;gap:12px;margin-left:8px;">
            <span style="color:{meta_color};cursor:pointer;font-size:14px;">＋</span>
            <span style="color:{meta_color};cursor:pointer;font-size:14px;">↗</span>
            <span style="color:{meta_color};cursor:pointer;font-size:14px;">⋮</span>
        </div>
    </div>
    """
