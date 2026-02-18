# dashboard/config.py
import os

def get_db_url():
    """Retourne l'URL de connexion selon l'environnement."""
    # Streamlit Cloud
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and "database" in st.secrets:
            return st.secrets["database"]["url"]
    except:
        pass
    
    # Variable d'environnement
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    
    # Local
    return "postgresql://admin:password123@localhost:5432/crypto_db"