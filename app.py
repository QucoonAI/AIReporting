import streamlit as st
from main import get_response

st.set_page_config(page_title="Supermarket Analytics Chatbot", layout="wide")

if 'messages' not in st.session_state:
    st.session_state.messages = []

st.title("🛒 Supermarket Analytics Chatbot")

user_input = st.chat_input("Ask me about your store data, sales, inventory, etc.")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    answer = get_response(user_input)
    st.session_state.messages.append({"role": "assistant", "content": answer})

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
