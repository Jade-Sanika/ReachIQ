import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

class Config:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Optional additional settings (for Flask apps)
    SECRET_KEY = os.getenv("SECRET_KEY", "default_secret_key")
    DEBUG = os.getenv("DEBUG", "True").lower() == "true"

    # Safety check
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("❌ Supabase credentials not found in environment variables.")
