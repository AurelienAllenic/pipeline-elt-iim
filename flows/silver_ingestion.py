from io import BytesIO
from pathlib import Path

from prefect import flow, task

import pandas as pd

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="read_from_bronze", retries=2)
def read_from_bronze_layer(object_name: str) -> pd.DataFrame:
    """
    Read CSV data from bronze bucket.

    Args:
        object_name: Name of object in MinIO bronze bucket

    Returns:
        DataFrame with the data
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_BRONZE):
        raise ValueError(f"Bucket {BUCKET_BRONZE} does not exist")

    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))
    print(f"Read {object_name} from {BUCKET_BRONZE} ({len(df)} rows)")
    return df


@task(name="transform_to_silver", retries=2)
def transform_to_silver_layer(df: pd.DataFrame, file_type: str = "clients") -> pd.DataFrame:
    """
    Transform data: clean, standardize, normalize, deduplicate.

    Args:
        df: DataFrame to transform
        file_type: Type of file ('clients' or 'achats')

    Returns:
        Transformed DataFrame
    """
    if file_type == "clients":
        df = transform_clients_data(df)
    elif file_type == "achats":
        df = transform_achats_data(df)
    
    return df


@task(name="save_to_silver", retries=2)
def save_to_silver_layer(df: pd.DataFrame, object_name: str) -> str:
    """
    Save transformed DataFrame to silver bucket.

    Args:
        df: DataFrame to save
        object_name: Name of object in MinIO silver bucket

    Returns:
        Object name in silver layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # Sauvegarder le DataFrame en CSV en mémoire
    silver_csv = BytesIO()
    df.to_csv(silver_csv, index=False, encoding='utf-8')
    silver_csv.seek(0)

    client.put_object(
        BUCKET_SILVER,
        object_name,
        silver_csv,
        length=silver_csv.getbuffer().nbytes
    )
    print(f"Saved {object_name} to {BUCKET_SILVER} ({len(df)} rows)")
    return object_name


def transform_clients_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform clients data: clean nulls and outliers, standardize dates, 
    normalize data types, deduplicate records.
    """
    initial_count = len(df)
    dates_standardized = False
    types_normalized = False

    # 1. Nettoyer les valeurs nulles et aberrantes
    # Supprimer les lignes avec des valeurs nulles dans les colonnes critiques
    df = df.dropna(subset=['id_client', 'nom', 'email', 'date_inscription'])

    # Nettoyer les emails invalides (format basique)
    df = df[df['email'].str.contains('@', na=False)]

    # 2. Standardiser les formats de dates
    if 'date_inscription' in df.columns:
        df['date_inscription'] = pd.to_datetime(df['date_inscription'], errors='coerce')
        df = df[df['date_inscription'] <= pd.Timestamp.now()]
        # Standardiser le format de date (YYYY-MM-DD)
        df['date_inscription'] = df['date_inscription'].dt.strftime('%Y-%m-%d')
        dates_standardized = True

    # 3. Normaliser les types de données
    if 'id_client' in df.columns:
        df['id_client'] = df['id_client'].astype('int64')
    if 'nom' in df.columns:
        df['nom'] = df['nom'].astype('string')
    if 'email' in df.columns:
        df['email'] = df['email'].astype('string')
    if 'date_inscription' in df.columns:
        df['date_inscription'] = df['date_inscription'].astype('string')
    if 'pays' in df.columns:
        df['pays'] = df['pays'].astype('string')
    types_normalized = True

    # 4. Dédupliquer les enregistrements
    before_dedup_count = len(df)
    duplicates_count = df.duplicated(subset=['id_client']).sum()
    df = df.drop_duplicates(subset=['id_client'], keep='first')
    after_dedup_count = len(df)

    removed_count = initial_count - before_dedup_count
    final_count = len(df)

    # Messages de confirmation
    if removed_count > 0:
        print(f"✓ Nettoyage clients: {removed_count} lignes supprimées ({initial_count} → {before_dedup_count})")
    else:
        print(f"✓ Nettoyage clients: Aucune ligne supprimée ({before_dedup_count} lignes valides)")

    if dates_standardized:
        print(f"✓ Standardisation dates clients: Format unifié (YYYY-MM-DD)")

    if types_normalized:
        types_info = "id_client(int), nom(string), email(string), date_inscription(string), pays(string)"
        print(f"✓ Normalisation types clients: {types_info}")

    if duplicates_count > 0:
        print(f"✓ Déduplication clients: {duplicates_count} doublons supprimés ({before_dedup_count} → {after_dedup_count})")
    else:
        print(f"✓ Déduplication clients: Aucun doublon détecté ({after_dedup_count} enregistrements uniques)")

    return df


def transform_achats_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform achats data: clean nulls and outliers, standardize dates,
    normalize data types, deduplicate records.
    """
    initial_count = len(df)
    dates_standardized = False
    types_normalized = False

    # 1. Nettoyer les valeurs nulles et aberrantes
    # Supprimer les lignes avec des valeurs nulles dans les colonnes critiques
    df = df.dropna(subset=['id_achat', 'id_client', 'date_achat', 'montant'])

    # Nettoyer les montants aberrants (négatifs ou trop élevés)
    if 'montant' in df.columns:
        # Supprimer les montants négatifs
        df = df[df['montant'] >= 0]
        # Supprimer les montants trop élevés (outliers > 99e percentile)
        q99 = df['montant'].quantile(0.99)
        df = df[df['montant'] <= q99 * 2]  # Garder jusqu'à 2x le 99e percentile

    # 2. Standardiser les formats de dates
    if 'date_achat' in df.columns:
        df['date_achat'] = pd.to_datetime(df['date_achat'], errors='coerce')
        df = df[df['date_achat'] <= pd.Timestamp.now()]
        # Supprimer les dates trop anciennes (plus de 10 ans)
        min_date = pd.Timestamp.now() - pd.Timedelta(days=3650)
        df = df[df['date_achat'] >= min_date]
        # Standardiser le format de date (YYYY-MM-DD)
        df['date_achat'] = df['date_achat'].dt.strftime('%Y-%m-%d')
        dates_standardized = True

    # 3. Normaliser les types de données
    if 'id_achat' in df.columns:
        df['id_achat'] = df['id_achat'].astype('int64')
    if 'id_client' in df.columns:
        df['id_client'] = df['id_client'].astype('int64')
    if 'date_achat' in df.columns:
        df['date_achat'] = df['date_achat'].astype('string')
    if 'montant' in df.columns:
        df['montant'] = df['montant'].astype('float64')
    if 'produit' in df.columns:
        df['produit'] = df['produit'].astype('string')
    types_normalized = True

    # 4. Dédupliquer les enregistrements
    before_dedup_count = len(df)
    duplicates_count = df.duplicated(subset=['id_achat']).sum()
    df = df.drop_duplicates(subset=['id_achat'], keep='first')
    after_dedup_count = len(df)

    removed_count = initial_count - before_dedup_count
    final_count = len(df)

    # Messages de confirmation
    if removed_count > 0:
        print(f"✓ Nettoyage achats: {removed_count} lignes supprimées ({initial_count} → {before_dedup_count})")
    else:
        print(f"✓ Nettoyage achats: Aucune ligne supprimée ({before_dedup_count} lignes valides)")

    if dates_standardized:
        print(f"✓ Standardisation dates achats: Format unifié (YYYY-MM-DD)")

    if types_normalized:
        types_info = "id_achat(int), id_client(int), date_achat(string), montant(float), produit(string)"
        print(f"✓ Normalisation types achats: {types_info}")

    if duplicates_count > 0:
        print(f"✓ Déduplication achats: {duplicates_count} doublons supprimés ({before_dedup_count} → {after_dedup_count})")
    else:
        print(f"✓ Déduplication achats: Aucun doublon détecté ({after_dedup_count} enregistrements uniques)")

    return df


@flow(name="Silver Transformation Flow")
def silver_ingestion_flow() -> dict:
    """
    Main flow: Read data from bronze, transform it, and save to silver layer.

    Returns:
        Dictionary with transformed file names
    """
    # Lire les données depuis bronze
    clients_df = read_from_bronze_layer("clients.csv")
    achats_df = read_from_bronze_layer("achats.csv")

    # Transformer les données
    transformed_clients = transform_to_silver_layer(clients_df, file_type="clients")
    transformed_achats = transform_to_silver_layer(achats_df, file_type="achats")

    # Sauvegarder dans silver
    silver_clients = save_to_silver_layer(transformed_clients, "clients.csv")
    silver_achats = save_to_silver_layer(transformed_achats, "achats.csv")

    print("\n" + "="*50)
    print("✓ SILVER TRANSFORMATION TERMINÉE AVEC SUCCÈS")
    print("="*50)
    print(f"  • Fichiers clients: {silver_clients}")
    print(f"  • Fichiers achats: {silver_achats}")
    print(f"  • Nettoyage: Valeurs nulles et aberrantes supprimées")
    print(f"  • Standardisation: Formats de dates unifiés (YYYY-MM-DD)")
    print(f"  • Normalisation: Types de données normalisés (int, float, string)")
    print(f"  • Déduplication: Enregistrements dupliqués supprimés")
    print(f"  • Tous les fichiers ont été transformés et sauvegardés dans le bucket 'silver'")
    print("="*50 + "\n")

    return {
        "clients": silver_clients,
        "achats": silver_achats
    }


if __name__ == "__main__":
    result = silver_ingestion_flow()
    print(f"Silver transformation complete: {result}")
