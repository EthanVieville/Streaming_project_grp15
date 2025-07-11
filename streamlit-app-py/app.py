import streamlit as st
from streamlit_autorefresh import st_autorefresh

import pandas as pd
import matplotlib.pyplot as plt
import base64
import os

import psycopg2
from sqlalchemy import create_engine


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

# ====================== LOAD FULL DATA =======================
df = pd.read_sql_query("SELECT * FROM marriages", engine)
df['age_at_marriage'] = pd.to_numeric(df['age_at_marriage'], errors='coerce')
df['children_count'] = pd.to_numeric(df['children_count'], errors='coerce')
df['years_since_marriage'] = pd.to_numeric(df['years_since_marriage'], errors='coerce')


# ====================== CLOSE DB =============================
cursor.close()
conn.close()
engine.dispose()


# ==============
# *** FRONT ***
# =============

# ====================== AUTOREFRESH ==========================
if len(df) == 100_000:
    st_autorefresh(interval=5000, limit=None, key="autorefresh")


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


# ====================== VISUALS - INTRO ==============================
st.info(f"{len(df)} / 10 000 loaded.")



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
    st.warning("Table note created (yet).")

ax3.set_ylabel("Total")
st.pyplot(fig3)


# ========== SATISFACTION PAR MARIAGE (non divorcé) ==========
st.title("Statistiques Satisfaction par Type de Mariage (non divorcés)")

try:
    # Lire la table complète (une seule version en base)
    df_stats = pd.read_sql_query("SELECT * FROM satisfaction_by_marriage_type", engine)

    if not df_stats.empty:
        st.subheader("Répartition Satisfaction (Love vs. Arranged)")

        # Affichage brut
        st.dataframe(df_stats)

        # Pivot pour graphique empilé
        pivot_df = df_stats.pivot(index='Marriage_Type', columns='Marital_Satisfaction', values='Count').fillna(0)

        fig, ax = plt.subplots(figsize=(8, 6))
        pivot_df.plot(kind='bar', stacked=True, ax=ax, colormap='Set2')
        ax.set_ylabel("Nombre de personnes")
        ax.set_title("Satisfaction selon le type de mariage (hors divorces)")
        ax.legend(title="Satisfaction")
        st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_marriage_type empty (for now hehe).")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)

# ========== SATISFACTION PAR TRANCHE D'AGE (à âge du mariage - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR TRANCHE D'AGE (à âge du mariage - non divorcé - marriage arrangé)")

try:
    df_age = pd.read_sql_query("SELECT * FROM satisfaction_by_age_range", engine)

    if not df_age.empty:
        st.dataframe(df_age)

        pivot = df_age.pivot(index="Age_Range", columns="Marital_Satisfaction", values="Count").fillna(0)
        fig, ax = plt.subplots(figsize=(10, 6))
        pivot.plot(kind="bar", stacked=True, ax=ax)
        ax.set_ylabel("Nombre de personnes")
        ax.set_title("Satisfaction selon l'âge au moment du mariage")
        st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_age_range empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)

# ========== SATISFACTION PAR EDUCATION (education level - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR EDUCATION (education level - non divorcé - marriage arrangé)")

try:
    df_edu = pd.read_sql_query("SELECT * FROM satisfaction_by_education_level", engine)

    if not df_edu.empty:
        st.dataframe(df_edu)

        pivot = df_edu.pivot(index="Education_Level", columns="Marital_Satisfaction", values="Count").fillna(0)
        fig, ax = plt.subplots(figsize=(10, 6))
        pivot.plot(kind="bar", stacked=True, ax=ax)
        ax.set_ylabel("Nombre de personnes")
        ax.set_title("Satisfaction selon le niveau d'éducation")
        st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_education_level empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)


# ========== SATISFACTION PAR RELGION (religion - non divorcé - marriage arrangé) ==========


st.title("Satisfaction par Religion (Mariages Arrangés, non divorcés)")
st.title("???????????????????? RACISTEEEEEEEEEEEE")

try:
    df_religion = pd.read_sql_query("SELECT * FROM satisfaction_by_religion_arranged", engine)

    if not df_religion.empty:
        st.dataframe(df_religion)

        grouped = df_religion.groupby("Religion")
        n = len(grouped)
        fig, axs = plt.subplots(nrows=(n + 1) // 2, ncols=2, figsize=(14, 6), squeeze=False)
        axs = axs.flatten()

        for i, (religion, group) in enumerate(grouped):
            ax = axs[i]
            labels = group["Marital_Satisfaction"]
            sizes = group["Count"]
            ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
            ax.axis("equal")
            ax.set_title(religion)

        # Remove unused subplots
        for j in range(i + 1, len(axs)):
            fig.delaxes(axs[j])

        st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_religion_arranged empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)

# ========== SATISFACTION PAR INTER-RELGION (religion - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR INTER-RELGION (religion - non divorcé - marriage arrangé)")

try:
    df_inter_religion = pd.read_sql_query("SELECT * FROM satisfaction_by_inter_religion", engine)

    if not df_inter_religion.empty:
        st.dataframe(df_inter_religion)

        pivot = df_inter_religion.pivot(index="Religion", columns="Marital_Satisfaction", values="Count").fillna(0)

        fig, ax = plt.subplots(figsize=(10, 6))
        im = ax.imshow(pivot.values, cmap="YlGnBu")

        ax.set_xticks(range(len(pivot.columns)))
        ax.set_yticks(range(len(pivot.index)))
        ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
        ax.set_yticklabels(pivot.index)
        ax.set_title("Heatmap : Satisfaction par Religion (Inter-Religion)")

        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                ax.text(j, i, int(pivot.iloc[i, j]), ha="center", va="center", color="black")

        fig.colorbar(im, ax=ax)
        st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_inter_religion empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)


# ========== SATISFACTION PAR REVNUS (revenus - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR REVNUS (revenus - non divorcé - marriage arrangé)")

try:
    df_income = pd.read_sql_query("SELECT * FROM satisfaction_by_income_level", engine)

    if not df_income.empty:
        st.dataframe(df_income)

        pivot = df_income.pivot(index="Income_Level", columns="Marital_Satisfaction", values="Count").fillna(0)

        fig, ax = plt.subplots(figsize=(10, 6))
        pivot.plot(kind='barh', stacked=True, ax=ax, colormap="viridis")

        ax.set_xlabel("Nombre de personnes")
        ax.set_ylabel("Niveau de revenu")
        ax.set_title("Satisfaction par revenu (mariages arrangés)")
        st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_income_level empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)

# ========== SATISFACTION PAR APPROBATION PARENTAL (papa mama - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR APPROBATION daddy (papa mama - non divorcé - marriage arrangé)")

try:
    df_approval = pd.read_sql_query("SELECT * FROM satisfaction_by_parental_approval", engine)

    if not df_approval.empty:
        st.dataframe(df_approval)

        grouped = df_approval.groupby("Parental_Approval")

        for label, group in grouped:
            st.subheader(f"Approbation parentale : {label}")
            fig, ax = plt.subplots()
            ax.pie(group["Count"], labels=group["Marital_Satisfaction"], autopct="%1.1f%%", startangle=90)
            ax.axis('equal')
            st.pyplot(fig)
    else:
        st.info("Table satisfaction_by_parental_approval empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)


# *** 
st.title("Partie 2 : Ce qu'il ne faut pas faire.")

# ***

# ========== DIVORCE PAR RELIGION (religin - divorcé - marriage arrangé) ==========

st.title("Taux de Divorce par Religion (Mariages Arrangés)")

try:
    df_div = pd.read_sql_query("SELECT * FROM divorces_by_religion", engine)

    if not df_div.empty:
        st.dataframe(df_div)

        fig, ax = plt.subplots(figsize=(8, 5))
        df_div_sorted = df_div.sort_values("Count", ascending=True)
        ax.barh(df_div_sorted["Religion"], df_div_sorted["Count"], color="crimson")
        ax.set_xlabel("Nombre de divorces")
        ax.set_ylabel("Religion")
        ax.set_title("Divorces par religion (mariages arrangés)")
        st.pyplot(fig)
    else:
        st.info("Table divorces_by_religion empty.")
except Exception as e:
     st.warning("Tables not created (yet).")
    #st.exception(e)


# ========== DIVORCE PAR EDUCATION (education - divorcé - marriage arrangé) ==========

st.title("Taux de Divorce par Niveau d'Éducation (Mariages Arrangés)")

try:
    df_div_edu = pd.read_sql_query("SELECT * FROM divorces_by_education", engine)

    if not df_div_edu.empty:
        st.dataframe(df_div_edu)

        fig, ax = plt.subplots(figsize=(7, 7))
        ax.pie(
            df_div_edu["Count"],
            labels=df_div_edu["Education_Level"],
            autopct="%1.1f%%",
            startangle=90,
            colors=plt.cm.Paired.colors
        )
        ax.set_title("Répartition des divorces par niveau d'éducation (arrangés)")
        st.pyplot(fig)
    else:
      st.info("Table divorces_by_education empty.")
except Exception as e:
     st.warning("Tables not created (yet).")
    #st.exception(e)


# ========== DIVORCE PAR SPOSUE WORKING (spouse - divorcé - marriage arrangé) ==========


st.title("Divorces selon le statut professionnel du conjoint (Mariages Arrangés)")

try:
    df_spouse = pd.read_sql_query("SELECT * FROM divorces_by_spouse_working", engine)

    if not df_spouse.empty:
        st.dataframe(df_spouse)

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(df_spouse["Spouse_Working"], df_spouse["Count"], color="salmon")
        ax.set_xlabel("Nombre de divorces")
        ax.set_ylabel("Conjoint travaille ?")
        ax.set_title("Divorces par statut du conjoint (arrangé)")
        st.pyplot(fig)
    else:
       st.info("Table divorces_by_education empty.")
except Exception as e:
     st.warning("Tables not created (yet).")
    #st.exception(e)


# ========== ALORS ???? LOVE SOLUTION : ==========

st.title("Alors alors alors ?")

st.markdown("""
-  Postgraduate (pas si con, mais pas trop intelligent)
-  Religion: Muslim (cheh bardella)
-  Income: Middle (0 michto)
-  Parental Approval: High (euh cool ?)
-  Age at Marriage: 27–30 (top)
-  Spouse Working: No (bosser avec son conjoint NONONONONON)
""")


st.title("Combien d'enfants ? ")


try:
    df_kids = pd.read_sql_query("SELECT * FROM children_count_by_profile", engine)

    if not df_kids.empty:
        st.dataframe(df_kids)

        fig, ax = plt.subplots()
        ax.bar(df_kids["Children_Count"], df_kids["Count"], color="purple")
        ax.set_xlabel("Nombre d'enfants")
        ax.set_ylabel("Nombre de couples")
        ax.set_title("Répartition des enfants pour le profil (arrangé, 27-30, etc...)")
        st.pyplot(fig)
    else:
        st.info("Table children_count_by_profile empty.")
except Exception as e:
     st.warning("Tables not created (yet).")
    #st.exception(e)