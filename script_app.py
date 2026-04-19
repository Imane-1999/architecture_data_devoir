import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# Configuration de base de l'app
st.set_page_config(layout="wide", page_title="LinkedIn Job Insights")

st.title("Rapport LinkedIn : Marché de l'emploi")
st.markdown("Analyse interactive basée sur les données de Linkedin.")

# Récupération de la session Snowflake
session = get_active_session()

# On met les données en cache pour éviter de requêter Snowflake à chaque clic
@st.cache_data
def fetch_data(query):
    return session.sql(query).to_pandas()

# Chargement initial des vues globales
df_taille_entreprise = fetch_data("SELECT * FROM linkedin.gold.fact_postings_by_company_size")
df_secteurs = fetch_data("SELECT * FROM linkedin.gold.fact_postings_by_industry ORDER BY TOTAL_OFFERS DESC LIMIT 20")
df_type_contrat = fetch_data("SELECT * FROM linkedin.gold.fact_postings_by_work_type")

# Organisation de l'interface par onglets
onglet1, onglet2, onglet3 = st.tabs([
    "Rôles & Salaires", 
    "Tailles & Contrats", 
    "Top Secteurs"
])

with onglet1:
    st.header("Analyse des postes et rémunérations par secteur")
    
    # On récupère la liste des secteurs pour alimenter la liste déroulante
    liste_secteurs = fetch_data("SELECT DISTINCT INDUSTRY FROM linkedin.gold.fact_job_analysis_by_industry WHERE INDUSTRY IS NOT NULL ORDER BY INDUSTRY")
    
    secteur_choisi = st.selectbox("Filtrez par secteur d'activité :", liste_secteurs['INDUSTRY'])
    
    if secteur_choisi:
        secteur_safe = secteur_choisi.replace("'", "''")
        
        requete = f"""
            SELECT TITLE, TOTAL_POSTINGS, PEAK_SALARY, AVG_MAX_SALARY
            FROM linkedin.gold.fact_job_analysis_by_industry
            WHERE INDUSTRY = '{secteur_safe}'
        """
        df_filtre = fetch_data(requete)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(f"Top 10 des postes - {secteur_choisi}")
            top_postes = df_filtre.nlargest(10, 'TOTAL_POSTINGS')
            
            graph_postes = alt.Chart(top_postes).mark_bar(color='#0077b5').encode(
                x=alt.X('TOTAL_POSTINGS:Q', title="Nombre d'offres"),
                y=alt.Y('TITLE:N', sort='-x', title='Titre du poste'),
                tooltip=['TITLE', 'TOTAL_POSTINGS']
            ).properties(height=400)
            
            st.altair_chart(graph_postes, use_container_width=True)

        with col2:
            st.subheader(f"Top 10 des salaires - {secteur_choisi}")
            # On exclut les postes sans salaire renseignée avant de trier
            df_salaires = df_filtre.dropna(subset=['PEAK_SALARY'])
            top_salaires = df_salaires.nlargest(10, 'PEAK_SALARY')
            
            graph_salaires = alt.Chart(top_salaires).mark_bar(color='#00a0dc').encode(
                x=alt.X('PEAK_SALARY:Q', title='Salaire Max (USD)'),
                y=alt.Y('TITLE:N', sort='-x', title='Titre du poste'),
                tooltip=['TITLE', 'PEAK_SALARY', 'TOTAL_POSTINGS']
            ).properties(height=400)
            
            st.altair_chart(graph_salaires, use_container_width=True)

with onglet2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("Offres par taille d'entreprise")
        st.markdown("*(0 = TPE, 7 = Multinationale)*")
        
        graph_taille = alt.Chart(df_taille_entreprise).mark_bar(color='#313335').encode(
            x=alt.X('COMPANY_SIZE:O', title='Catégorie de taille'),
            y=alt.Y('JOB_COUNT:Q', title="Volume d'offres"),
            tooltip=['COMPANY_SIZE', 'JOB_COUNT']
        ).properties(height=400)
        
        st.altair_chart(graph_taille, use_container_width=True)
        
    with col2:
        st.header("Offres par type de contrat")
        st.markdown("*(Temps plein, stage, freelance...)*")
        
        graph_contrats = alt.Chart(df_type_contrat).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="JOB_COUNT", type="quantitative"),
            color=alt.Color(field="FORMATTED_WORK_TYPE", type="nominal", title="Type de contrat"),
            tooltip=['FORMATTED_WORK_TYPE', 'JOB_COUNT']
        ).properties(height=400)
        
        st.altair_chart(graph_contrats, use_container_width=True)

with onglet3:
    st.header("Secteurs les plus dynamiques")
    st.markdown("Top 20 des industries qui publient le plus d'offres.")
    
    # On limite aux 20 premières pour garder un graphique lisible et repondre à la demande d'analyse
    graph_ind = alt.Chart(df_secteurs.head(20)).mark_bar(color='#86B817').encode(
        x=alt.X('INDUSTRY:N', sort='-y', title="Secteur d'activité", axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('TOTAL_OFFERS:Q', title="Nombre total d'offres"),
        tooltip=['INDUSTRY', 'TOTAL_OFFERS']
    ).properties(height=500)
    
    st.altair_chart(graph_ind, use_container_width=True)