import streamlit as st
from streamlit_autorefresh import st_autorefresh

import pandas as pd
import matplotlib.pyplot as plt
import base64
import os

import psycopg2
from sqlalchemy import create_engine

# ====================== AUTOREFRESH ==========================
st_autorefresh(interval=5000, limit=None, key="autorefresh")  # every 5s

# ====================== TITRE ================================
st.title("LOVE IS LOVE IS HATE IS LOVE YEAH INDIAAAAAA YALAAAA")

# ====================== CLEAN UI =============================
st.markdown("""
    <style>
    #MainMenu, footer, header {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# ====================== BACKGROUND IMAGE =====================
def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

img_base64 = get_base64_image("final.png")

st.markdown(
    f"""
    <style>
    html, body, .stApp {{
        background: url("data:image/png;base64,{img_base64}");
        background-repeat: no-repeat;
        background-position: center center;
        background-attachment: fixed;
        background-size: cover;
    }}
    .main > div {{
        background-color: rgba(255, 255, 255, 0.9); 
        padding: 2rem;
        border-radius: 16px;
        margin: 2rem;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# ====================== CONNECT TO DB ========================
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="mariage_db",
        user="postgres",
        password="1234"
    )
    engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/mariage_db")

    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'marriages'
        )
    """)
    exists = cursor.fetchone()[0]

    if not exists:
        st.warning("Table 'marriages' not found (yet).")
        st.stop()

except psycopg2.DatabaseError as e:
    st.error(f"Erreur de base de données : {e}")
    st.stop()

# ========== UTILITY: Get last update timestamp ===============
def get_last_update_timestamp(table_name: str, engine) -> int:
    query = f"SELECT last_updated FROM meta WHERE table_name = '{table_name}'"
    try:
        result = pd.read_sql_query(query, engine)
        if not result.empty:
            return int(result.iloc[0]['last_updated'])
    except:
        pass
    return 0

# ========== INIT STATE FOR TIMESTAMP COMPARISON =============
if "last_stats_update" not in st.session_state:
    st.session_state.last_stats_update = 0

# ====================== LOAD FULL DATA =======================
df = pd.read_sql_query("SELECT * FROM marriages", engine)
df['age_at_marriage'] = pd.to_numeric(df['age_at_marriage'], errors='coerce')
df['children_count'] = pd.to_numeric(df['children_count'], errors='coerce')
df['years_since_marriage'] = pd.to_numeric(df['years_since_marriage'], errors='coerce')

# ====================== STATS TABLE ==========================
df_stats = pd.DataFrame()
current_update = get_last_update_timestamp("satisfactionByMarriageType", engine)

if True:
    try:
        query = """
        SELECT * FROM satisfaction_by_marriage_type
        WHERE updated_at = (SELECT MAX(updated_at) FROM satisfaction_by_marriage_type)
        """
        df_stats = pd.read_sql_query(query, engine)
        st.session_state.last_stats_update = current_update
    except Exception as e:
        st.warning("Data incomming...")

# ====================== CLOSE DB =============================
cursor.close()
conn.close()
engine.dispose()

# ====================== VISUALS - INTRO ==============================
st.subheader("Mariage type : Love vs. Arranged")
type_counts = df['marriage_type'].value_counts()
fig1, ax1 = plt.subplots()
ax1.bar(type_counts.index, type_counts.values, color=['skyblue', 'orange'])
ax1.set_ylabel("Total")
st.pyplot(fig1)


st.subheader("Satisfaction")
cross_tab = pd.crosstab(df['marriage_type'], df['marital_satisfaction'])
st.dataframe(cross_tab)

fig3, ax3 = plt.subplots()
if not cross_tab.empty and cross_tab.select_dtypes(include='number').shape[1] > 0:
    cross_tab.T.plot(kind='bar', stacked=True, ax=ax3)
else:
    st.warning("Data incomming...")

ax3.set_ylabel("Total")
st.pyplot(fig3)


# ========== SATISFACTION PAR MARIAGE (non divorcé) ==========
st.title("Statistiques Satisfaction par Type de Mariage (pour non divorcé bien sûr...)")

try:
    df_stats = pd.read_sql_query("SELECT * FROM satisfaction_by_marriage_type", engine)

    if not df_stats.empty:
        st.subheader("Satisfaction par type de mariage (non divorcés)")
        st.dataframe(df_stats)

        pivot_df = df_stats.pivot(index='Marriage_Type', columns='Marital_Satisfaction', values='count').fillna(0)

        fig, ax = plt.subplots(figsize=(10,6))
        pivot_df.plot(kind='bar', stacked=True, ax=ax)
        ax.set_ylabel("Nombre de personnes")
        ax.set_title("Répartition de la satisfaction")
        st.pyplot(fig)
    else:
        st.info("Stats incomming...")
except Exception:
    st.warning("(le graph arrive pannique pas)")
