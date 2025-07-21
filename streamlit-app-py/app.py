# GESTION IMPORTS
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import base64
import plotly.express as px
from sqlalchemy import create_engine, text

import logging


# INIT LOGGER
logging.basicConfig(level=logging.INFO)


COLORS = {
    'arranged': '#FF6B6B',
    'love': '#4ECDC4', 
    'primary': '#667eea',
    'secondary': '#764ba2'
}

    
# ====================== CONFIGURATION PAGE =================
st.set_page_config(
    page_title="Love vs. Arranged (arranged first)",
    page_icon="💕",
    layout="wide"
)

# ====================== STYLES PERSONNALISÉS ================
st.markdown("""
    <style>
    /* Masquer le menu Streamlit */
    #MainMenu, footer, header {visibility: hidden;}
    
    /* Style principal */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    

    
    .section-divider {
        border: none;
        height: 3px;
        background: linear-gradient(90deg, #667eea, #764ba2);
        margin: 3rem 0;
        border-radius: 2px;
    }
    
    /* Navigation tabs style */
    .nav-tabs {
        display: flex;
        justify-content: center;
        gap: 1rem;
        margin: 2rem 0;
    }
    
    .nav-tab {
        padding: 0.8rem 1.5rem;
        background: white;
        border: 2px solid #667eea;
        border-radius: 25px;
        color: #667eea;
        text-decoration: none;
        font-weight: 600;
        transition: all 0.3s ease;
        cursor: pointer;
    }
    
    .nav-tab.active {
        background: #667eea;
        color: white;
    }
    
    .nav-tab:hover {
        transform: translateY(-1px);
        box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);
    }
    
    /* Background */
    .stApp {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    </style>
""", unsafe_allow_html=True)

# ====================== HEADER PRINCIPAL ====================
st.markdown("""
    <div class="main-header">
        <h1>Le Mariage Arrangé c'est top !</h1>
        <p style="font-size: 1.2rem; margin-top: 1rem;">Analyse Bonheur Marital entre mariage arrangé et par amour. Erreurs à éviter !</p>
    </div>
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

    """,
    unsafe_allow_html=True
)



# ====================== CONNECT TO DB ========================
@st.cache_resource
def init_db_connection():
    try:
        engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/mariage_db")
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'marriages'
                )
            """))
            if not result.fetchone()[0]:
                st.warning("Table 'marriages' non trouvée.")
                st.stop()
        return engine
    except Exception as e:
        st.error("Impossible de se connecter à la base de données.")
        st.stop()

engine = init_db_connection()


@st.cache_data(ttl=30)
def load_data_from_db(query):
    try:
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        logging.warning(f"Erreur requête SQL: {e}")
        return pd.DataFrame()

# ====================== LOAD FULL DATA =======================
df = load_data_from_db("SELECT * FROM marriages")
if df.empty:
    st.error("Pas encore de données en tables 'marriages' (ca arrive).")
    st.stop()

df['age_at_marriage'] = pd.to_numeric(df['age_at_marriage'], errors='coerce')
df['children_count'] = pd.to_numeric(df['children_count'], errors='coerce')
df['years_since_marriage'] = pd.to_numeric(df['years_since_marriage'], errors='coerce')


# ====================== CLOSE DB =============================
#cursor.close()
#conn.close()
engine.dispose()


# ====================== AUTOREFRESH ==========================
if len(df) != 10_000:
    st_autorefresh(interval=5000, limit=None, key="autorefresh")




# ====================== VISUALS - INTRO ==============================
# ====================== VUE D'ENSEMBLE ==============

# Chargement des données principales
df = load_data_from_db("SELECT * FROM marriages")
df['age_at_marriage'] = pd.to_numeric(df['age_at_marriage'], errors='coerce')
df['children_count'] = pd.to_numeric(df['children_count'], errors='coerce')
df['years_since_marriage'] = pd.to_numeric(df['years_since_marriage'], errors='coerce')

st.markdown('<div id="overview"></div>', unsafe_allow_html=True)
st.markdown("## Vue d'ensemble")

# Métriques principales
col1, col2, col3, col4 = st.columns(4)

with col1:

    progress = len(df) / 10000
    st.metric(
        label="Progression", 
        value=f"{len(df):,} / 10,000",
        delta=f"{progress*100:.1f}% complété"
    )
    st.progress(progress)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    arranged_pct = (df['marriage_type'].value_counts().get('Arranged', 0) / len(df) * 100)
    st.metric(
        label="Mariages Arrangés", 
        value=f"{arranged_pct:.1f}%",
        delta=f"{100-arranged_pct:.1f}% d'amour"
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    avg_age = df['age_at_marriage'].mean()
    st.metric(
        label="Age Moyen Mariage", 
        value=f"{avg_age:.1f} ans",
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    divorce_rate = (df['divorce_status'] == 'Yes').sum() / len(df) * 100
    st.metric(
        label="Taux de Divorce", 
        value=f"{divorce_rate:.1f}%",
    )
    st.markdown('</div>', unsafe_allow_html=True)

# Graphiques principaux côte à côte
col1, col2 = st.columns(2)

with col1:
    st.subheader("Répartition des types")
    
    type_counts = df['marriage_type'].value_counts().reset_index()
    type_counts.columns = ['marriage_type', 'Total']
    
    fig = px.pie(
        type_counts,
        values='Total',
        names='marriage_type',
        color_discrete_sequence=[COLORS['arranged'], COLORS['love']],
        hole=0.4
    )
    
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        textfont_size=14
    )
    
    fig.update_layout(
        showlegend=True,
        template='plotly_white',
        height=350
    )
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.subheader("Distribution des âges")
    
    fig = px.histogram(
        df,
        x='age_at_marriage',
        color='marriage_type',
        nbins=20,
        color_discrete_sequence=[COLORS['arranged'], COLORS['love']],
        title=""
    )
    
    fig.update_layout(
        template='plotly_white',
        height=350,
        xaxis_title="Âge au mariage",
        yaxis_title="Nombre de personnes"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)



# ========== SATISFACTION PAR MARIAGE (non divorcé) ==========

st.title("Statistiques Satisfaction par Type de Mariage (non divorcés)")

try:
    df_stats = load_data_from_db("SELECT * FROM satisfaction_by_marriage_type")

    if not df_stats.empty:
        st.subheader("Répartition Satisfaction (Love vs. Arranged)")

        st.dataframe(df_stats)

        fig = px.bar(
            df_stats,
            x="marital_satisfaction",
            y="count",
            color="marriage_type",
            barmode="group",
            text="count",
            title="Satisfaction selon le type de mariage (hors divorces)",
            color_discrete_sequence=['#69b3a2', '#ff9933']
        )

        fig.update_layout(
            xaxis_title="Niveau de satisfaction",
            yaxis_title="Nombre de personnes",
            legend_title="Type de mariage",
            template="simple_white",
            title_x=0.5
        )

        fig.update_traces(textposition="outside")

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Table satisfaction_by_marriage_type empty (for now hehe).")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)

# ========== SATISFACTION PAR TRANCHE D'AGE (à âge du mariage - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR TRANCHE D'AGE (à âge du mariage - non divorcé - par type de mariage)")

try:
    df_age = load_data_from_db("SELECT * FROM satisfaction_by_age_range")

    if not df_age.empty:
        st.dataframe(df_age)

        fig = px.bar(
            df_age,
            x="age_range",
            y="count",
            color="marital_satisfaction",
            facet_col="marriage_type",
            barmode="stack",
            text="count",
            category_orders={"age_range": sorted(df_age["age_range"].unique())},
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={
                "age_range": "Tranche d'âge au mariage",
                "count": "Nombre de personnes",
                "marital_satisfaction": "Satisfaction",
                "marriage_type": "Type de mariage"
            },
            title="Satisfaction par âge au mariage et type de mariage"
        )

        fig.update_layout(
            title_x=0.5,
            legend_title_text="Satisfaction",
            margin=dict(t=60, b=40),
            template="simple_white",
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        fig.update_traces(textposition="inside")

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Table satisfaction_by_age_range empty.")
except Exception as e:
    st.warning("Tables not created (yet).")
    #st.exception(e)

# ========== SATISFACTION PAR EDUCATION (education level - non divorcé - marriage arrangé) ==========

st.title("SATISFACTION PAR EDUCATION ET TYPE DE MARIAGE (non divorcé - par type de mariage)")

try:
    df_edu = load_data_from_db("SELECT * FROM satisfaction_by_education_level")

    if not df_edu.empty:
        st.dataframe(df_edu)

        fig = px.bar(
            df_edu,
            y="education_level",
            x="count",
            color="marital_satisfaction",
            facet_col="marriage_type",
            barmode="group",
            orientation="h",
            color_discrete_map={
                "Satisfied": "#2ca02c",
                "Neutral": "#ff7f0e",
                "Dissatisfied": "#d62728"
            },
            labels={
                "education_level": "Niveau d'éducation",
                "count": "Nombre de personnes",
                "marital_satisfaction": "Satisfaction",
                "marriage_type": "Type de mariage"
            },
            title="Satisfaction selon le niveau d'éducation et le type de mariage"
        )

        fig.update_traces(texttemplate='%{x}', textposition='outside')
        fig.update_layout(
            title_x=0.5,
            legend_title_text="Satisfaction",
            template="plotly_white",
            margin=dict(t=60, b=40),
            yaxis=dict(autorange="reversed")
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Table satisfaction_by_education_level empty.")
except Exception as e:
    st.warning(f"Erreur : {e}")
    #st.exception(e)


# ========== SATISFACTION PAR RELGION (religion - non divorcé - marriage arrangé) ==========

st.title("Satisfaction par religion : Mariages Arrangés vs Mariages d'Amour")

try:
    df_religion = load_data_from_db("SELECT * FROM satisfaction_by_religion_arranged")

    if not df_religion.empty:
        st.dataframe(df_religion)

        # Graphique clair : religion en X, marriage_type en couleur, motifs pour satisfaction
        fig = px.bar(
            df_religion,
            x="religion",
            y="count",
            color="marriage_type",  # Arranged vs Love
            pattern_shape="marital_satisfaction",  # Motifs pour satisfaction
            barmode="group",
            labels={
                "count": "Nombre de personnes",
                "religion": "religion",
                "marriage_type": "Type de mariage",
                "marital_satisfaction": "Satisfaction"
            },
            title="Satisfaction par religion : Comparaison Arranged vs Love",
            color_discrete_map={
                "Arranged": "#FF6B6B",  # Rouge
                "Love": "#4ECDC4"       # Bleu
            }
        )

        fig.update_layout(
            height=700,
            xaxis_tickangle=45,
            margin=dict(b=150, t=100),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            )
        )

        # Ajouter les valeurs sur les barres
        fig.update_traces(
            textposition='outside',
            texttemplate='%{y}',
            textfont_size=9
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Table satisfaction_by_religion_arranged vide.")

except Exception as e:
    st.warning(f"Erreur : {e}")
    #st.exception(e)

# ========== SATISFACTION PAR REVNUS (revenus - non divorcé - marriage arrangé) ==========

st.title("Satisfaction par Revenus : Mariages Arrangés vs Amour")

try:
    df_income = load_data_from_db("SELECT * FROM satisfaction_by_income_level")

    if not df_income.empty:
        with st.expander("Voir les données"):
            st.dataframe(df_income)

        # Graphique principal : barres groupées par type de mariage
        fig = px.bar(
            df_income,
            x="income_level",
            y="count",
            color="marriage_type",
            pattern_shape="marital_satisfaction",
            barmode="group",
            labels={
                "count": "Nombre de personnes",
                "income_level": "Niveau de revenu",
                "marriage_type": "Type de mariage",
                "marital_satisfaction": "Satisfaction"
            },
            title="Satisfaction par niveau de revenu",
            color_discrete_map={
                "Arranged": "#FF6B6B",  # Rouge
                "Love": "#4ECDC4"       # Bleu
            }
        )

        fig.update_layout(
            height=500,
            xaxis_tickangle=45,
            margin=dict(b=100)
        )

        fig.update_traces(textposition='outside', texttemplate='%{y}', textfont_size=9)

        st.plotly_chart(fig, use_container_width=True)

        # Légende simple
        col1, col2 = st.columns(2)
        with col1:
            st.info("🔴 **Rouge** = Mariages arrangés")
        with col2:
            st.info("🔵 **Bleu** = Mariages d'amour")

    else:
        st.info("Table satisfaction_by_income_level vide.")

except Exception as e:
    st.warning(f"Erreur : {e}")
    #st.exception(e)

# ==================================================================================

# ====================== SECTION DIVORCES ====================
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown('<div id="divorces"></div>', unsafe_allow_html=True)
st.markdown("## Quelques tips pour pas divorcer quand même ..")

# ========== DIVORCE PAR religion (religin - divorcé - marriage arrangé) ==========

st.title("Taux de Divorce par religion (Mariage Type)")

try:
    df_div = load_data_from_db("SELECT * FROM divorces_by_religion")

    if not df_div.empty:
        st.dataframe(df_div)

        # Barres groupées : une par type de mariage, groupées par religion
        fig = px.bar(
            df_div,
            x="count",
            y="religion",
            color="marriage_type",
            orientation="h",
            text="count",
            barmode="group",
            color_discrete_map={
                "Arranged": "#FF7F0E",
                "Love": "#1F77B4"
            },
            labels={
                "count": "Nombre de divorces",
                "religion": "religion",
                "marriage_type": "Type de mariage"
            },
            title="Nombre de divorces par religion et type de mariage"
        )

        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=600
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Table divorces_by_religion vide.")
except Exception as e:
    st.warning(f"Erreur : {e}")
    #st.exception(e)


# ========== DIVORCE PAR EDUCATION (education - divorcé - marriage arrangé) ==========

st.title("Taux de Divorce par Niveau d'Éducation (Mariage Type)")

try:
    df_div_edu = load_data_from_db("SELECT * FROM divorces_by_education")

    if not df_div_edu.empty:
        st.dataframe(df_div_edu)

        # Barres groupées : niveau d'éducation vs type de mariage
        fig = px.bar(
            df_div_edu,
            x="education_level",
            y="count",
            color="marriage_type",
            barmode="group",
            text="count",
            color_discrete_map={
                "Arranged": "#FF7F0E",
                "Love": "#1F77B4"
            },
            labels={
                "count": "Nombre de divorces",
                "education_level": "Niveau d'éducation",
                "marriage_type": "Type de mariage"
            },
            title="Divorces par niveau d'éducation et type de mariage"
        )

        fig.update_layout(
            xaxis_tickangle=-45,
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Table divorces_by_education vide.")
except Exception as e:
    st.warning(f"Erreur : {e}")
    #st.exception(e)


# ========== DIVORCE PAR SPOSUE WORKING (spouse - divorcé - marriage arrangé) ==========


st.title("Se marier avec ses collègues ? nan mais on est ou la")

try:
    df_spouse = load_data_from_db("SELECT * FROM divorces_by_spouse_working")

    if not df_spouse.empty:
        st.dataframe(df_spouse)

        fig = px.bar(
            df_spouse,
            x="count",
            y="spouse_working",
            color="marriage_type",
            orientation="h",
            text="count",
            barmode="group",
            color_discrete_map={
                "Arranged": "#EF553B",
                "Love": "#636EFA"
            },
            labels={
                "count": "Nombre de divorces",
                "spouse_working": "Conjoint travaille ?",
                "marriage_type": "Type de mariage"
            },
            title="Divorces selon le spouse working situation"
        )

        fig.update_layout(
            height=500,
            yaxis=dict(categoryorder='total ascending')
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Table divorces_by_spouse_working vide.")
except Exception as e:
    st.warning(f"Erreur : {e}")
    #st.exception(e)



# ========== ALORS ???? ==========

st.title("Alors alors alors ?")

st.markdown("""
N'hésite plus !
Commande ton truc sur amazon !!
""")

