import os
from dotenv import load_dotenv
load_dotenv()
os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL")

from io import BytesIO
from prefect import flow, task
from prefect.logging import get_run_logger
import pandas as pd
from pymongo import MongoClient
import time
from datetime import datetime

try:
    from .config import BUCKET_GOLD, get_minio_client, MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION_PREFIX
except ImportError:
    from config import BUCKET_GOLD, get_minio_client, MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION_PREFIX


@task(name="read_parquet_from_gold", retries=2)
def read_parquet_from_gold(object_name: str) -> pd.DataFrame:
    """Lit un fichier Parquet depuis le bucket Gold"""
    logger = get_run_logger()
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_GOLD):
        logger.error(f"Bucket {BUCKET_GOLD} does not exist")
        raise ValueError(f"Bucket {BUCKET_GOLD} does not exist")

    if object_name.endswith('.csv'):
        response = client.get_object(BUCKET_GOLD, object_name)
        df = pd.read_csv(BytesIO(response.read()))
    else:
        response = client.get_object(BUCKET_GOLD, object_name)
        df = pd.read_parquet(BytesIO(response.read()))
    
    response.close()
    response.release_conn()
    
    logger.info(f"Read {object_name} from {BUCKET_GOLD} ({len(df)} rows)")
    return df


@task(name="write_to_mongodb", retries=2)
def write_to_mongodb(df: pd.DataFrame, collection_name: str) -> str:
    """Écrit un DataFrame dans MongoDB et enregistre les métadonnées de timing"""
    logger = get_run_logger()
    
    if not MONGODB_URI:
        raise ValueError("MONGODB_URI must be set in .env")
    
    # Mesurer le temps d'écriture
    start_time = time.time()
    timestamp_write_start = datetime.now().isoformat()
    
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    collection = db[collection_name]

    records = df.to_dict('records')

    collection.delete_many({})
    if records:
        collection.insert_many(records)

    end_time = time.time()
    timestamp_write_end = datetime.now().isoformat()
    duration = end_time - start_time

    logger.info(f"Wrote {len(records)} documents to MongoDB collection '{collection_name}'")
    logger.info(f"Timestamp écriture début: {timestamp_write_start}")
    logger.info(f"Timestamp écriture fin: {timestamp_write_end}")
    logger.info(f"Durée écriture: {duration:.3f} secondes")
    
    # Sauvegarder les métadonnées de refresh dans une collection spéciale
    try:
        metadata_collection = db["_refresh_metadata"]
        metadata_collection.insert_one({
            "collection": collection_name,
            "write_start": timestamp_write_start,
            "write_end": timestamp_write_end,
            "duration_seconds": duration,
            "record_count": len(records),
            "timestamp": datetime.now().isoformat()
        })
        logger.info(f"Métadonnées de refresh enregistrées pour '{collection_name}'")
    except Exception as e:
        logger.warning(f"Impossible d'enregistrer les métadonnées de refresh: {e}")
    
    client.close()

    return collection_name


@flow(name="Gold to MongoDB Flow")
def gold_to_mongodb_flow() -> dict:
    """Flow qui lit depuis Gold et écrit dans MongoDB"""
    logger = get_run_logger()

    files_to_export = [
        "fact_achats.csv",
        "kpis.csv",
        "dim_clients.csv",
        "dim_produits.csv",
        "dim_dates.csv",
        "agg_jour.csv",
        "agg_semaine.csv",
        "agg_mois.csv",
        "ca_par_pays.csv"
    ]
    
    results = {}
    
    for file_name in files_to_export:
        try:
            df = read_parquet_from_gold(file_name)

            collection_name = MONGODB_COLLECTION_PREFIX + file_name.replace('.csv', '')

            collection = write_to_mongodb(df, collection_name)
            results[file_name] = collection
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement de {file_name}: {e}")
            results[file_name] = f"ERROR: {str(e)}"
    
    logger.info("="*50)
    logger.info("✓ EXPORT VERS MONGODB TERMINÉ")
    logger.info("="*50)
    logger.info(f"Collections créées: {len([r for r in results.values() if not r.startswith('ERROR')])}")
    logger.info("="*50)
    
    return results


if __name__ == "__main__":
    result = gold_to_mongodb_flow()
    print(f"Export MongoDB complete: {result}")
