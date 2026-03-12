import streamlit as st
import plotly.graph_objects as go

st.write("Is testing pie charts")
fig = go.Figure(go.Pie(labels=['Pérdida', 'Resto'], values=[10, 90]))
fig.update_layout(clickmode='event+select')
event = st.plotly_chart(fig, on_select="rerun", selection_mode=["points"], key="pie1")
st.write("Event Output:", event)
