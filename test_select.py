import streamlit as st
import plotly.graph_objects as go

if 'show_children' not in st.session_state:
    st.session_state.show_children = False

if not st.session_state.show_children:
    fig = go.Figure(go.Pie(labels=['Pérdida', 'Resto'], values=[10, 90]))
    event = st.plotly_chart(fig, on_select="rerun", key="pie1")
    st.write("Event data:", event)
    if event and event.selection.get("points"):
        if event.selection["points"][0]["label"] == "Pérdida":
            st.session_state.show_children = True
            st.rerun()
else:
    st.button("Back")
