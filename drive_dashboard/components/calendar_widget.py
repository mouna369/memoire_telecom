import streamlit as st
import calendar
from datetime import date

def render_calendar():
    dm = st.session_state.dark_mode
    bg = "#1A1D2E" if dm else "#FFFFFF"
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"
    day_hover = "rgba(79,110,247,0.1)"

    today = date.today()
    year, month = today.year, today.month
    cal = calendar.monthcalendar(year, month)
    month_name = today.strftime("%B %Y")

    # Tab buttons
    tabs_html = ""
    for tab in ["Week", "Month", "Year"]:
        active = tab == "Month"
        tabs_html += f"""
        <button style="background:{'#4F6EF7' if active else 'transparent'};color:{'white' if active else muted};
                       border:none;border-radius:8px;padding:6px 14px;font-size:12px;font-weight:600;
                       cursor:pointer;font-family:'Plus Jakarta Sans',sans-serif;">{tab}</button>
        """

    # Day headers
    days_html = ""
    for d in ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]:
        days_html += f'<div style="text-align:center;color:{muted};font-size:11px;font-weight:600;padding:4px 0;">{d}</div>'

    # Weeks
    weeks_html = ""
    for week in cal:
        for day in week:
            if day == 0:
                weeks_html += '<div></div>'
            elif day == today.day:
                weeks_html += f'<div style="width:30px;height:30px;background:#4F6EF7;border-radius:50%;display:flex;align-items:center;justify-content:center;color:white;font-size:12px;font-weight:700;margin:auto;cursor:pointer;">{day}</div>'
            else:
                weeks_html += f'<div style="text-align:center;color:{text};font-size:12px;padding:6px 0;cursor:pointer;border-radius:6px;transition:background 0.2s;" onmouseover="this.style.background=\'{day_hover}\'" onmouseout="this.style.background=\'transparent\'">{day}</div>'

    st.markdown(f"""
    <div style="background:{bg};border-radius:18px;padding:20px;border:1px solid {border};">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
            <span style="color:{text};font-size:16px;font-weight:700;">Calendar</span>
            <span style="color:#4F6EF7;font-size:16px;cursor:pointer;">∧</span>
        </div>
        <div style="display:flex;gap:4px;background:rgba(255,255,255,0.04);border-radius:10px;padding:4px;margin-bottom:16px;">
            {tabs_html}
        </div>
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
            <span style="color:{muted};font-size:13px;cursor:pointer;">‹</span>
            <span style="color:{text};font-size:13px;font-weight:600;">{month_name}</span>
            <span style="color:{muted};font-size:13px;cursor:pointer;">›</span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:4px;">
            {days_html}
            {weeks_html}
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_tasks():
    dm = st.session_state.dark_mode
    bg = "#1A1D2E" if dm else "#FFFFFF"
    text = "#FFFFFF" if dm else "#1A1D2E"
    muted = "#8B92A5"
    border = "rgba(255,255,255,0.05)" if dm else "#E8EAF2"

    tasks = [
        {"title": "Review health care app with team", "date": "7 Dec 2024 | 10:00 AM",
         "color": "#F05454", "members": [("#4F6EF7","J"),("#F5A623","M"),("#2ECC71","K")]},
        {"title": "Update design system docs", "date": "8 Dec 2024 | 2:00 PM",
         "color": "#4F6EF7", "members": [("#F05454","A"),("#9B59B6","B")]},
        {"title": "Client presentation prep", "date": "9 Dec 2024 | 9:00 AM",
         "color": "#F5A623", "members": [("#2ECC71","C"),("#4F6EF7","D"),("#F05454","E")]},
    ]

    task_items = ""
    for t in tasks:
        avatars = "".join([f'<div style="width:22px;height:22px;background:{m[0]};border-radius:50%;display:flex;align-items:center;justify-content:center;color:white;font-size:9px;font-weight:700;margin-left:-5px;">{m[1]}</div>' for m in t["members"]])
        task_items += f"""
        <div style="display:flex;align-items:center;gap:12px;padding:12px 0;
                    border-bottom:1px solid {border};">
            <div style="width:4px;height:44px;background:{t['color']};border-radius:4px;flex-shrink:0;"></div>
            <div style="flex:1;">
                <div style="color:{text};font-size:13px;font-weight:600;margin-bottom:4px;">{t['title']}</div>
                <div style="color:{muted};font-size:11px;">🕐 {t['date']}</div>
                <div style="display:flex;align-items:center;margin-top:6px;margin-left:5px;">{avatars}</div>
            </div>
            <span style="color:{muted};cursor:pointer;font-size:16px;">⋮</span>
        </div>
        """

    st.markdown(f"""
    <div style="background:{bg};border-radius:18px;padding:20px;border:1px solid {border};margin-top:16px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
            <span style="color:{text};font-size:16px;font-weight:700;">Your Task</span>
            <span style="color:#4F6EF7;font-size:13px;font-weight:600;cursor:pointer;">View All ›</span>
        </div>
        {task_items}
    </div>
    """, unsafe_allow_html=True)
