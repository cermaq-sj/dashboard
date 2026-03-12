import streamlit as st
import plotly.graph_objects as go
from streamlit_plotly_events import plotly_events

fig = go.Figure(go.Pie(labels=['Pérdida', 'Resto'], values=[10, 90]))
clicked = plotly_events(fig, click_event=True, key="pie1")
st.write("Clicked:", clicked)
