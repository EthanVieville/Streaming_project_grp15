import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import base64


st.title("LOVE IS LOVE IS HATE IS LOVE YEAH INDIAAAAAA YALAAAA")


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

# FULL GPT GENERATED, yeah. 

# Connexion BDD SQLite
conn = sqlite3.connect("../marriages.db")
df = pd.read_sql_query("SELECT * FROM marriages", conn)
conn.close()

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
