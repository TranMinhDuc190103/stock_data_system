import streamlit as st
from db_conn import DbConnection

st.set_page_config(page_title="Home", page_icon=":thumbsup:", layout="wide")

st.sidebar.success("Select a page below")