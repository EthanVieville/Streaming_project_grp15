import streamlit as st
from streamlit_autorefresh import st_autorefresh

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import base64


#count = st_autorefresh(interval=5000, limit=None, key="autorefresh")


st.title("LOVE IS LOVE IS HATE IS LOVE YEAH INDIAAAAAA YALAAAA")


#st.markdown(
#    """
#    <script>
#    window.onscroll = function() {
#        sessionStorage.setItem("scrollPosition", window.scrollY);
#    };
#
#    window.onload = function() {
#        var scrollPos = sessionStorage.getItem("scrollPosition");
#        if (scrollPos) {
#            window.scrollTo(0, parseInt(scrollPos));
#        }
#    };
#    </script>
#    """, unsafe_allow_html=True
#)




st.markdown(
    """
    <style>
    /* Cache le menu Streamlit (≡) */
    #MainMenu {visibility: hidden;}

    /* Cache le footer Streamlit */
    footer {visibility: hidden;}

    /* Optionnel : cache aussi le bandeau en haut si tu veux quelque chose de clean */
    header {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True
)



# INTEGRATION DE RAJ 
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
        background-top: 10;
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

# ===================== GET DATA ==================================
# FULL GPT GENERATED, yeah. 

# Connexion BDD SQLite
conn = sqlite3.connect("../marriages.db")
# SELECT * for all the basic dataset
df = pd.read_sql_query("SELECT * FROM marriages", conn)

# Lire la table des stats (celle mise à jour par Spark)
#query = "SELECT * FROM satisfactionByMarriageType"
#df_stats = pd.read_sql_query(query, conn)

# Satisfaction level for arranged marriage N not divorced (yet)
query = """
SELECT * FROM satisfactionByMarriageType
WHERE updated_at = (SELECT MAX(updated_at) FROM satisfactionByMarriageType)
"""
df_stats = pd.read_sql_query(query, conn)

# Satisfaction level for age (at marriage) bracket, not divorced
query = """
SELECT * FROM satisfactionByAgeBracket
WHERE updated_at = (SELECT MAX(updated_at) FROM satisfactionByAgeBracket)
"""
df_age = pd.read_sql_query(query, conn)

conn.close()
# =====================================================================

# Transfo data supplémentaire ...
df['Age_at_Marriage'] = pd.to_numeric(df['Age_at_Marriage'], errors='coerce')
df['Children_Count'] = pd.to_numeric(df['Children_Count'], errors='coerce')
df['Years_Since_Marriage'] = pd.to_numeric(df['Years_Since_Marriage'], errors='coerce')
# // ....

st.subheader("Mariage type : Love vs. Arranged")
type_counts = df['Marriage_Type'].value_counts()
fig1, ax1 = plt.subplots()
ax1.bar(type_counts.index, type_counts.values, color=['skyblue', 'orange'])
ax1.set_ylabel("Total")
st.pyplot(fig1)

st.subheader("Age at marriage (ish) : Love vs. Arranged")
age_mean = df.groupby('Marriage_Type')['Age_at_Marriage'].mean().dropna()
fig2, ax2 = plt.subplots()
ax2.bar(age_mean.index, age_mean.values, color=['skyblue', 'orange'])
ax2.set_ylabel("Mean Age")
st.pyplot(fig2)

st.subheader("Satisfaction")
cross_tab = pd.crosstab(df['Marriage_Type'], df['Marital_Satisfaction'])
st.dataframe(cross_tab)

fig3, ax3 = plt.subplots()
cross_tab.T.plot(kind='bar', stacked=True, ax=ax3)
ax3.set_ylabel("Total")
st.pyplot(fig3)


#=======================================
# FROM query 

st.title("Statistiques Satisfaction par Type de Mariage")

# Afficher le tableau des stats
st.subheader("Données de satisfaction par type de mariage")
st.dataframe(df_stats)

# Création du graphique (barres empilées par niveau de satisfaction)
pivot_df = df_stats.pivot(index='Marriage_Type', columns='Marital_Satisfaction', values='count').fillna(0)

fig, ax = plt.subplots(figsize=(10,6))
pivot_df.plot(kind='bar', stacked=True, ax=ax, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])

ax.set_ylabel("Nombre de personnes")
ax.set_title("Répartition de la satisfaction par type de mariage")
ax.legend(title="Satisfaction")

st.pyplot(fig)

# =========
st.subheader("Répartition satisfaction par tranche d'âge au mariage")
st.dataframe(df_age)

pivot_age = df_age.pivot(index='age_bracket', columns='Marital_Satisfaction', values='count').fillna(0)
pivot_age = pivot_age.apply(pd.to_numeric, errors='coerce').fillna(0)


fig, ax = plt.subplots(figsize=(10,6))
pivot_age.plot(kind='bar', stacked=True, ax=ax, color=['#1f77b4', '#ff7f0e', '#2ca02c'])
ax.set_ylabel("Nombre de personnes")
ax.set_title("Satisfaction par tranche d'âge au moment du mariage")
ax.legend(title="Satisfaction")
st.pyplot(fig)