from io import BytesIO
from pathlib import Path

from prefect import flow, task

import pandas as pd

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="read_from_silver", retries=2)
def read_from_silver_layer(object_name: str) -> pd.DataFrame:
    """
    Read CSV data from silver bucket.

    Args:
        object_name: Name of object in MinIO silver bucket

    Returns:
        DataFrame with the data
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        raise ValueError(f"Bucket {BUCKET_SILVER} does not exist")

    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))
    print(f"Read {object_name} from {BUCKET_SILVER} ({len(df)} rows)")
    return df


@task(name="join_data", retries=2)
def join_clients_and_achats(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join clients and achats data to create a fact table.

    Args:
        clients_df: DataFrame with clients data
        achats_df: DataFrame with achats data

    Returns:
        Joined DataFrame (fact table)
    """
    # Joindre les données sur id_client
    fact_table = achats_df.merge(
        clients_df,
        on='id_client',
        how='left',
        suffixes=('', '_client')
    )
    
    print(f"Joined data: {len(fact_table)} rows (from {len(achats_df)} achats and {len(clients_df)} clients)")
    return fact_table


@task(name="calculate_kpis", retries=2)
def calculate_kpis(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate key performance indicators (KPIs).

    Args:
        fact_table: Joined DataFrame with clients and achats

    Returns:
        DataFrame with KPIs
    """
    kpis = {}
    
    # Convertir les dates en datetime pour les calculs
    fact_table['date_achat'] = pd.to_datetime(fact_table['date_achat'])
    
    # 1. CA total (Chiffre d'Affaires)
    kpis['ca_total'] = fact_table['montant'].sum()
    
    # 2. Nombre total d'achats
    kpis['nb_achats_total'] = len(fact_table)
    
    # 3. Panier moyen
    kpis['panier_moyen'] = fact_table['montant'].mean()
    
    # 4. Nombre de clients uniques
    kpis['nb_clients_uniques'] = fact_table['id_client'].nunique()
    
    # 5. Montant moyen par client
    ca_par_client = fact_table.groupby('id_client')['montant'].sum()
    kpis['montant_moyen_par_client'] = ca_par_client.mean()
    
    # 6. Taux de croissance (comparaison mois actuel vs mois précédent)
    fact_table['annee_mois'] = fact_table['date_achat'].dt.to_period('M')
    ca_par_mois = fact_table.groupby('annee_mois')['montant'].sum().sort_index()
    if len(ca_par_mois) >= 2:
        dernier_mois = ca_par_mois.iloc[-1]
        mois_precedent = ca_par_mois.iloc[-2]
        kpis['taux_croissance_mensuel'] = ((dernier_mois - mois_precedent) / mois_precedent) * 100
    else:
        kpis['taux_croissance_mensuel'] = None
    
    # 7. Distribution statistique des montants
    kpis['montant_median'] = fact_table['montant'].median()
    kpis['montant_std'] = fact_table['montant'].std()
    kpis['montant_min'] = fact_table['montant'].min()
    kpis['montant_max'] = fact_table['montant'].max()
    
    # Créer un DataFrame avec les KPIs
    kpis_df = pd.DataFrame([kpis])
    
    print("\n" + "="*50)
    print("KPIs CALCULÉS:")
    print("="*50)
    for key, value in kpis.items():
        if value is not None:
            if isinstance(value, float):
                print(f"  • {key}: {value:,.2f}")
            else:
                print(f"  • {key}: {value:,}")
    print("="*50)
    
    return kpis_df


@task(name="create_dimension_tables", retries=2)
def create_dimension_tables(clients_df: pd.DataFrame, fact_table: pd.DataFrame) -> dict:
    """
    Create dimension tables (dim_clients, dim_produits, dim_dates).

    Args:
        clients_df: DataFrame with clients data
        fact_table: Fact table with joined data

    Returns:
        Dictionary with dimension tables
    """
    dimensions = {}
    
    # Dimension Clients
    dim_clients = clients_df.copy()
    dimensions['dim_clients'] = dim_clients
    print(f"✓ Dimension Clients créée: {len(dim_clients)} clients")
    
    # Dimension Produits
    dim_produits = fact_table[['produit']].drop_duplicates().reset_index(drop=True)
    dim_produits['id_produit'] = range(1, len(dim_produits) + 1)
    dim_produits = dim_produits[['id_produit', 'produit']]
    dimensions['dim_produits'] = dim_produits
    print(f"✓ Dimension Produits créée: {len(dim_produits)} produits")
    
    # Dimension Dates (avec agrégations temporelles)
    fact_table['date_achat'] = pd.to_datetime(fact_table['date_achat'])
    dates_unique = fact_table['date_achat'].dt.date.unique()
    dim_dates = pd.DataFrame({'date': dates_unique})
    dim_dates['date'] = pd.to_datetime(dim_dates['date'])
    dim_dates['jour'] = dim_dates['date'].dt.day
    dim_dates['mois'] = dim_dates['date'].dt.month
    dim_dates['annee'] = dim_dates['date'].dt.year
    dim_dates['jour_semaine'] = dim_dates['date'].dt.day_name()
    dim_dates['semaine'] = dim_dates['date'].dt.isocalendar().week
    dim_dates['trimestre'] = dim_dates['date'].dt.quarter
    dim_dates = dim_dates.sort_values('date').reset_index(drop=True)
    dim_dates['id_date'] = range(1, len(dim_dates) + 1)
    dimensions['dim_dates'] = dim_dates
    print(f"✓ Dimension Dates créée: {len(dim_dates)} dates avec agrégations temporelles")
    
    return dimensions


@task(name="calculate_temporal_aggregations", retries=2)
def calculate_temporal_aggregations(fact_table: pd.DataFrame) -> dict:
    """
    Calculate temporal aggregations (by day, week, month).

    Args:
        fact_table: Fact table with joined data

    Returns:
        Dictionary with temporal aggregations
    """
    fact_table['date_achat'] = pd.to_datetime(fact_table['date_achat'])
    
    aggregations = {}
    
    # Agrégation par jour
    agg_jour = fact_table.groupby(fact_table['date_achat'].dt.date).agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique'
    }).reset_index()
    agg_jour.columns = ['date', 'ca_total', 'panier_moyen', 'nb_achats', 'nb_clients']
    aggregations['agg_jour'] = agg_jour
    print(f"✓ Agrégation par jour: {len(agg_jour)} jours")
    
    # Agrégation par semaine
    fact_table['annee_semaine'] = fact_table['date_achat'].dt.to_period('W')
    agg_semaine = fact_table.groupby('annee_semaine').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique'
    }).reset_index()
    agg_semaine.columns = ['semaine', 'ca_total', 'panier_moyen', 'nb_achats', 'nb_clients']
    agg_semaine['semaine'] = agg_semaine['semaine'].astype(str)
    aggregations['agg_semaine'] = agg_semaine
    print(f"✓ Agrégation par semaine: {len(agg_semaine)} semaines")
    
    # Agrégation par mois
    fact_table['annee_mois'] = fact_table['date_achat'].dt.to_period('M')
    agg_mois = fact_table.groupby('annee_mois').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique'
    }).reset_index()
    agg_mois.columns = ['mois', 'ca_total', 'panier_moyen', 'nb_achats', 'nb_clients']
    agg_mois['mois'] = agg_mois['mois'].astype(str)
    aggregations['agg_mois'] = agg_mois
    print(f"✓ Agrégation par mois: {len(agg_mois)} mois")
    
    return aggregations


@task(name="calculate_ca_by_country", retries=2)
def calculate_ca_by_country(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate CA (Chiffre d'Affaires) by country.

    Args:
        fact_table: Fact table with joined data

    Returns:
        DataFrame with CA by country
    """
    ca_par_pays = fact_table.groupby('pays').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique'
    }).reset_index()
    ca_par_pays.columns = ['pays', 'ca_total', 'panier_moyen', 'nb_achats', 'nb_clients']
    ca_par_pays = ca_par_pays.sort_values('ca_total', ascending=False).reset_index(drop=True)
    
    print(f"✓ CA par pays calculé: {len(ca_par_pays)} pays")
    return ca_par_pays


@task(name="save_to_gold", retries=2)
def save_to_gold_layer(df: pd.DataFrame, object_name: str) -> str:
    """
    Save DataFrame to gold bucket.

    Args:
        df: DataFrame to save
        object_name: Name of object in MinIO gold bucket

    Returns:
        Object name in gold layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    # Sauvegarder le DataFrame en CSV en mémoire
    gold_csv = BytesIO()
    df.to_csv(gold_csv, index=False, encoding='utf-8')
    gold_csv.seek(0)

    client.put_object(
        BUCKET_GOLD,
        object_name,
        gold_csv,
        length=gold_csv.getbuffer().nbytes
    )
    print(f"Saved {object_name} to {BUCKET_GOLD} ({len(df)} rows)")
    return object_name


@flow(name="Gold Aggregation Flow")
def gold_ingestion_flow() -> dict:
    """
    Main flow: Read data from silver, calculate KPIs, create fact/dimension tables,
    calculate temporal aggregations, and save to gold layer.

    Returns:
        Dictionary with all created file names
    """
    # 1. Lire les données depuis silver
    clients_df = read_from_silver_layer("clients.csv")
    achats_df = read_from_silver_layer("achats.csv")

    # 2. Joindre les données pour créer la table de faits
    fact_table = join_clients_and_achats(clients_df, achats_df)
    
    # 3. Calculer les KPIs
    kpis_df = calculate_kpis(fact_table)
    
    # 4. Créer les tables de dimensions
    dimensions = create_dimension_tables(clients_df, fact_table)
    
    # 5. Calculer les agrégations temporelles
    temporal_aggs = calculate_temporal_aggregations(fact_table)
    
    # 6. Calculer le CA par pays
    ca_par_pays = calculate_ca_by_country(fact_table)
    
    # 7. Sauvegarder toutes les tables dans Gold
    saved_files = {}
    
    # Table de faits
    saved_files['fact_achats'] = save_to_gold_layer(fact_table, "fact_achats.csv")
    
    # KPIs
    saved_files['kpis'] = save_to_gold_layer(kpis_df, "kpis.csv")
    
    # Dimensions
    saved_files['dim_clients'] = save_to_gold_layer(dimensions['dim_clients'], "dim_clients.csv")
    saved_files['dim_produits'] = save_to_gold_layer(dimensions['dim_produits'], "dim_produits.csv")
    saved_files['dim_dates'] = save_to_gold_layer(dimensions['dim_dates'], "dim_dates.csv")
    
    # Agrégations temporelles
    saved_files['agg_jour'] = save_to_gold_layer(temporal_aggs['agg_jour'], "agg_jour.csv")
    saved_files['agg_semaine'] = save_to_gold_layer(temporal_aggs['agg_semaine'], "agg_semaine.csv")
    saved_files['agg_mois'] = save_to_gold_layer(temporal_aggs['agg_mois'], "agg_mois.csv")
    
    # CA par pays
    saved_files['ca_par_pays'] = save_to_gold_layer(ca_par_pays, "ca_par_pays.csv")

    print("\n" + "="*50)
    print("✓ GOLD AGGREGATION TERMINÉE AVEC SUCCÈS")
    print("="*50)
    print("  Tables créées:")
    print(f"    • Table de faits: fact_achats.csv")
    print(f"    • KPIs: kpis.csv")
    print(f"    • Dimensions: dim_clients.csv, dim_produits.csv, dim_dates.csv")
    print(f"    • Agrégations temporelles: agg_jour.csv, agg_semaine.csv, agg_mois.csv")
    print(f"    • CA par pays: ca_par_pays.csv")
    print("="*50 + "\n")

    return saved_files


if __name__ == "__main__":
    result = gold_ingestion_flow()
    print(f"Gold aggregation complete: {result}")
