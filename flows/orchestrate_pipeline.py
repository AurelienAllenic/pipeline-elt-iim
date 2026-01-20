import os
import sys
from pathlib import Path
from dotenv import load_dotenv

current_dir = Path(__file__).parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

load_dotenv()
os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL")

from prefect import flow
from prefect.logging import get_run_logger

try:
    from flows.bronze_ingestion import bronze_ingestion_flow
    from flows.silver_transformation import silver_ingestion_flow
    from flows.gold_agregation import gold_ingestion_flow
    from flows.gold_to_mongodb import gold_to_mongodb_flow
except ImportError:
    from .bronze_ingestion import bronze_ingestion_flow
    from .silver_transformation import silver_ingestion_flow
    from .gold_agregation import gold_ingestion_flow
    from .gold_to_mongodb import gold_to_mongodb_flow


@flow(name="ELT Pipeline Orchestrator", log_prints=True)
def elt_pipeline_orchestrator(data_dir: str = "./data/sources") -> dict:
    """
    Orchestrateur principal du pipeline ELT.
    Exécute les trois couches dans l'ordre : Bronze → Silver → Gold -> MongoDB.

    Args:
        data_dir: Répertoire contenant les fichiers sources CSV

    Returns:
        Dictionnaire avec les résultats de chaque couche
    """
    logger = get_run_logger()
    
    logger.info("="*60)
    logger.info("DÉMARRAGE DU PIPELINE ELT")
    logger.info("="*60)
    
    results = {}
    
    # 1. Couche Bronze
    logger.info("\n" + "="*60)
    logger.info("COUCHE BRONZE - Ingestion des données")
    logger.info("="*60)
    try:
        bronze_result = bronze_ingestion_flow(data_dir=data_dir)
        results["bronze"] = bronze_result
        logger.info(f"Bronze terminé : {bronze_result}")
    except Exception as e:
        logger.error(f"Erreur dans la couche Bronze : {e}")
        raise
    
    # 2. Couche Silver
    logger.info("\n" + "="*60)
    logger.info("COUCHE SILVER - Transformation des données")
    logger.info("="*60)
    try:
        silver_result = silver_ingestion_flow()
        results["silver"] = silver_result
        logger.info(f"Silver terminé : {silver_result}")
    except Exception as e:
        logger.error(f"Erreur dans la couche Silver : {e}")
        raise
    
    # 3. Couche Gold
    logger.info("\n" + "="*60)
    logger.info("COUCHE GOLD - Agrégations métier")
    logger.info("="*60)
    try:
        gold_result = gold_ingestion_flow()
        results["gold"] = gold_result
        logger.info(f"Gold terminé : {gold_result}")
    except Exception as e:
        logger.error(f"Erreur dans la couche Gold : {e}")
        raise

    # 4. Export Gold -> MongoDB
    logger.info("\n" + "="*60)
    logger.info("EXPORT MONGODB - Écriture des tables Gold")
    logger.info("="*60)
    try:
        mongo_result = gold_to_mongodb_flow()
        results["mongodb"] = mongo_result
        logger.info(f"MongoDB terminé : {mongo_result}")
    except Exception as e:
        logger.error(f"Erreur dans l'export MongoDB : {e}")
        raise
    
    # Résumé
    logger.info("\n" + "="*60)
    logger.info("PIPELINE ELT TERMINÉ AVEC SUCCÈS")
    logger.info("="*60)
    logger.info("Résumé des exécutions :")
    logger.info(f"  Bronze : {results.get('bronze', 'N/A')}")
    logger.info(f"  Silver : {results.get('silver', 'N/A')}")
    logger.info(f"  Gold   : {len(results.get('gold', {}))} tables créées")
    logger.info(f"  MongoDB: {len([r for r in results.get('mongodb', {}).values() if not str(r).startswith('ERROR')])} collections créées")
    logger.info("="*60)
    
    return results


if __name__ == "__main__":
    result = elt_pipeline_orchestrator()
    print("\n" + "="*60)
    print("Pipeline ELT complet terminé avec succès !")
    print("="*60)
    print(f"Résultats : {result}")
