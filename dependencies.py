from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pathlib import Path
import os
from kiteconnect import KiteConnect
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(BASE_DIR, "data")

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username="sysadmin",
    password="Apple@1239",
    host="trialnerror.in",
    port=5432,
    database="tfw",
)

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

from kiteconnect import KiteConnect
import pandas as pd
import numpy as np
from kiteconnect import KiteConnect
import requests

KITE_API_KEY = "tw96psyyds0yj8vj"
KITE_API_SECRET = "3iewov9onkbytzramkt263r9lvcdzks9"
ACCESS_TOKEN_API_URL = "http://kite.trialnerror.in/accesstoken/"


def kite_connect() -> tuple[KiteConnect, dict]:
    api_key = KITE_API_KEY
    api_secret = KITE_API_SECRET
    access_token_api_url = ACCESS_TOKEN_API_URL

    request = requests.get(access_token_api_url)
    access_token = request.json().get("access_token", "")
    status = "success"
    message = "Access token retrieved successfully."

    kite = KiteConnect(api_key=api_key)
    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
    except Exception as e:
        print("Error setting access token:", e)
        status = "error"
        message = "Error setting access token. " + str(e)

        loginurl = kite.login_url()
        kite = None
        return None, {"status": status, "message": message, "login_url": loginurl}

    return kite, {"status": status, "message": message, "data": profile}
