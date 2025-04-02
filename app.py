import streamlit as st
import json
import requests

# Streamlit UI Setup
st.set_page_config(page_title="Journal Search", layout="wide")

st.title("📚 AI-Powered Journal Search")
st.markdown("Enter a query below to find relevant journal articles.")

# User Input
queryPrompt = st.text_input("🔍 Enter your search query:", "")

if st.button("Search"):
    if not queryPrompt:
        st.warning("⚠️ Please enter a query.")
    else:
        # API Request to FastAPI
        api_url = "http://127.0.0.1:8000/custom-journal-query"  # Change if hosted elsewhere
        payload = {"queryPrompt": queryPrompt}

        with st.spinner("🔄 Searching for relevant journal articles..."):
            response = requests.post(api_url, json=payload)
        
        if response.status_code == 200:
            data = response.json()

            # Display Journal Context
            if "context" in data and data["context"] != "No relevant articles found.":
                st.subheader("📝 Retrieved Journal Articles")
                articles = json.loads(data["context"])

                for i, article in enumerate(articles, start=1):
                    with st.expander(f"📄 Article {i}: {article['article_url']}"):
                        st.write(f"**Authors:** {article['authors_text']}")
                        st.write(f"**Abstract:** {article['abstract_text']}")
                        st.write(f"🔗 [Read More]({article['article_url']})")

            else:
                st.warning("⚠️ No relevant journal articles found.")
        else:
            st.error("❌ Failed to fetch data. Please try again.")
