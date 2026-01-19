import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from flows.config import BUCKET_GOLD, get_minio_client

st.set_page_config(
    page_title="Dashboard ELT Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=300)
def load_data_from_minio(object_name: str) -> pd.DataFrame:
    """Charge un fichier CSV depuis le bucket Gold de MinIO"""
    try:
        client = get_minio_client()
        
        if not client.bucket_exists(BUCKET_GOLD):
            st.error(f"Le bucket {BUCKET_GOLD} n'existe pas")
            return pd.DataFrame()
        
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        df = pd.read_csv(BytesIO(data))
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement de {object_name}: {e}")
        return pd.DataFrame()


def main():
    st.title("📊 Dashboard ELT Pipeline")
    st.markdown("---")

    with st.spinner("Chargement des données depuis MinIO..."):
        kpis_df = load_data_from_minio("kpis.csv")
        fact_df = load_data_from_minio("fact_achats.csv")
        agg_jour_df = load_data_from_minio("agg_jour.csv")
        agg_semaine_df = load_data_from_minio("agg_semaine.csv")
        agg_mois_df = load_data_from_minio("agg_mois.csv")
        ca_par_pays_df = load_data_from_minio("ca_par_pays.csv")
        dim_produits_df = load_data_from_minio("dim_produits.csv")

    if kpis_df.empty:
        st.warning("⚠️ Aucune donnée disponible. Veuillez exécuter le pipeline ELT d'abord.")
        return
    
    # ========== SECTION 1: KPIs PRINCIPAUX ==========
    st.header("📈 Indicateurs Clés de Performance (KPIs)")
    
    if not kpis_df.empty:
        kpi = kpis_df.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="💰 CA Total",
                value=f"{kpi['ca_total']:,.2f} €" if pd.notna(kpi['ca_total']) else "N/A"
            )
        
        with col2:
            st.metric(
                label="🛒 Nombre d'achats",
                value=f"{int(kpi['nb_achats_total']):,}" if pd.notna(kpi['nb_achats_total']) else "N/A"
            )
        
        with col3:
            st.metric(
                label="💵 Panier moyen",
                value=f"{kpi['panier_moyen']:,.2f} €" if pd.notna(kpi['panier_moyen']) else "N/A"
            )
        
        with col4:
            croissance = kpi.get('taux_croissance_mensuel', 0)
            st.metric(
                label="📊 Croissance mensuelle",
                value=f"{croissance:.2f} %" if pd.notna(croissance) else "N/A",
                delta=f"{croissance:.2f} %" if pd.notna(croissance) and croissance != 0 else None
            )
        
        col5, col6, col7 = st.columns(3)
        
        with col5:
            st.metric(
                label="👥 Clients uniques",
                value=f"{int(kpi['nb_clients_uniques']):,}" if pd.notna(kpi['nb_clients_uniques']) else "N/A"
            )
        
        with col6:
            st.metric(
                label="💳 Montant moyen par client",
                value=f"{kpi['montant_moyen_par_client']:,.2f} €" if pd.notna(kpi['montant_moyen_par_client']) else "N/A"
            )
        
        with col7:
            st.metric(
                label="📉 Montant médian",
                value=f"{kpi['montant_median']:,.2f} €" if pd.notna(kpi['montant_median']) else "N/A"
            )
    
    st.markdown("---")
    
    # ========== SECTION 2: ÉVOLUTION TEMPORELLE ==========
    st.header("📅 Évolution Temporelle du Chiffre d'Affaires")

    granularite = st.selectbox(
        "Choisir la granularité temporelle",
        ["Par jour", "Par semaine", "Par mois"],
        key="granularite"
    )
    
    if granularite == "Par jour" and not agg_jour_df.empty:
        agg_jour_df['date'] = pd.to_datetime(agg_jour_df['date'])
        agg_jour_df = agg_jour_df.sort_values('date')
        
        fig = px.line(
            agg_jour_df,
            x='date',
            y='ca_total',
            title="Évolution du CA par jour",
            labels={'ca_total': 'CA Total (€)', 'date': 'Date'},
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.bar(
            agg_jour_df,
            x='date',
            y='nb_achats',
            title="Nombre d'achats par jour",
            labels={'nb_achats': 'Nombre d\'achats', 'date': 'Date'}
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
    
    elif granularite == "Par semaine" and not agg_semaine_df.empty:
        agg_semaine_df = agg_semaine_df.sort_values('semaine')
        
        fig = px.line(
            agg_semaine_df,
            x='semaine',
            y='ca_total',
            title="Évolution du CA par semaine",
            labels={'ca_total': 'CA Total (€)', 'semaine': 'Semaine'},
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        fig2 = px.bar(
            agg_semaine_df,
            x='semaine',
            y='nb_achats',
            title="Nombre d'achats par semaine",
            labels={'nb_achats': 'Nombre d\'achats', 'semaine': 'Semaine'}
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
    
    elif granularite == "Par mois" and not agg_mois_df.empty:
        agg_mois_df = agg_mois_df.sort_values('mois')
        
        fig = px.line(
            agg_mois_df,
            x='mois',
            y='ca_total',
            title="Évolution du CA par mois",
            labels={'ca_total': 'CA Total (€)', 'mois': 'Mois'},
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        fig2 = px.bar(
            agg_mois_df,
            x='mois',
            y='nb_achats',
            title="Nombre d'achats par mois",
            labels={'nb_achats': 'Nombre d\'achats', 'mois': 'Mois'}
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown("---")
    
    # ========== SECTION 3: ANALYSE PAR PRODUIT ==========
    st.header("🛍️ Analyse par Produit")
    
    if not fact_df.empty:
        # CA par produit
        ca_produit = fact_df.groupby('produit')['montant'].agg(['sum', 'count', 'mean']).reset_index()
        ca_produit.columns = ['Produit', 'CA Total (€)', 'Nombre d\'achats', 'Panier moyen (€)']
        ca_produit = ca_produit.sort_values('CA Total (€)', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                ca_produit,
                x='Produit',
                y='CA Total (€)',
                title="CA Total par Produit",
                color='CA Total (€)',
                color_continuous_scale='Blues'
            )
            fig.update_xaxes(tickangle=-45)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(
                ca_produit,
                values='CA Total (€)',
                names='Produit',
                title="Répartition du CA par Produit"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("📋 Détails par Produit")
        st.dataframe(ca_produit, use_container_width=True)
    
    st.markdown("---")
    
    # ========== SECTION 4: ANALYSE PAR PAYS ==========
    st.header("🌍 Analyse par Pays")
    
    if not ca_par_pays_df.empty:
        ca_par_pays_df = ca_par_pays_df.sort_values('ca_total', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                ca_par_pays_df,
                x='pays',
                y='ca_total',
                title="CA Total par Pays",
                labels={'ca_total': 'CA Total (€)', 'pays': 'Pays'},
                color='ca_total',
                color_continuous_scale='Greens'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(
                ca_par_pays_df,
                values='ca_total',
                names='pays',
                title="Répartition du CA par Pays"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Métriques par pays
        st.subheader("📊 Métriques par Pays")
        st.dataframe(
            ca_par_pays_df[['pays', 'ca_total', 'panier_moyen', 'nb_achats', 'nb_clients']].rename(columns={
                'pays': 'Pays',
                'ca_total': 'CA Total (€)',
                'panier_moyen': 'Panier Moyen (€)',
                'nb_achats': 'Nombre d\'achats',
                'nb_clients': 'Nombre de clients'
            }),
            use_container_width=True
        )
    
    st.markdown("---")
    
    # ========== SECTION 5: DISTRIBUTION DES MONTANTS ==========
    st.header("📊 Distribution des Montants")
    
    if not fact_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.histogram(
                fact_df,
                x='montant',
                nbins=50,
                title="Distribution des montants d'achat",
                labels={'montant': 'Montant (€)', 'count': 'Fréquence'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.box(
                fact_df,
                y='montant',
                title="Boîte à moustaches des montants",
                labels={'montant': 'Montant (€)'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ========== SECTION 6: DONNÉES BRUTES ==========
    with st.expander("📋 Voir les données brutes"):
        tab1, tab2, tab3, tab4 = st.tabs(["KPIs", "Fact Table", "Agrégations", "CA par Pays"])
        
        with tab1:
            st.dataframe(kpis_df, use_container_width=True)
        
        with tab2:
            st.dataframe(fact_df.head(100), use_container_width=True)
            st.caption(f"Affichage de 100 lignes sur {len(fact_df)} au total")
        
        with tab3:
            st.subheader("Par jour")
            st.dataframe(agg_jour_df, use_container_width=True)
            st.subheader("Par semaine")
            st.dataframe(agg_semaine_df, use_container_width=True)
            st.subheader("Par mois")
            st.dataframe(agg_mois_df, use_container_width=True)
        
        with tab4:
            st.dataframe(ca_par_pays_df, use_container_width=True)


if __name__ == "__main__":
    main()
