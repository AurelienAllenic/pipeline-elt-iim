import time
import httpx
from datetime import datetime
import os
from dotenv import load_dotenv
from prefect import task
from prefect.logging import get_run_logger

load_dotenv()
API_URL = os.getenv("API_URL", "http://localhost:8000")


@task(name="calculate_refresh_time", retries=1)
def calculate_refresh_time_task():
    logger = get_run_logger()
    
    logger.info("="*60)
    logger.info("CALCUL DU TEMPS DE REFRESH")
    logger.info("="*60)
    logger.info(f"API URL: {API_URL}")
    
    collections = [
        "kpis",
        "fact_achats",
        "ca_par_pays",
        "dim_clients",
        "dim_produits",
        "agg_jour"
    ]
    
    results = []
    connection_error = False
    
    for collection in collections:
        try:
            logger.info(f"Test de la collection: {collection}")
            response = httpx.get(f"{API_URL}/refresh_time/{collection}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if "error" in data:
                    logger.warning(f"  {data['error']}")
                    continue
                
                refresh_time = data.get("refresh_time_seconds", 0)
                write_duration = data.get("write_duration_seconds", 0)
                read_duration = data.get("read_duration_seconds", 0)
                record_count = data.get("record_count", 0)
                
                logger.info(f"  Temps de refresh: {refresh_time:.3f} secondes")
                logger.info(f"  Duree ecriture: {write_duration:.3f} secondes")
                logger.info(f"  Duree lecture: {read_duration:.3f} secondes")
                logger.info(f"  Nombre d'enregistrements: {record_count}")
                
                results.append({
                    "collection": collection,
                    "refresh_time": refresh_time,
                    "write_duration": write_duration,
                    "read_duration": read_duration,
                    "record_count": record_count
                })
            else:
                logger.error(f"  Erreur HTTP {response.status_code}: {response.text}")
                
        except httpx.RequestError as e:
            connection_error = True
            logger.warning(f"  Impossible de se connecter a l'API pour {collection}")
        except Exception as e:
            logger.error(f"  Erreur: {e}")
    
    if connection_error and not results:
        logger.warning("="*60)
        logger.warning("L'API FastAPI n'est pas disponible")
        logger.warning("="*60)
        logger.warning("Pour calculer le temps de refresh, lancez l'API dans un terminal separe:")
        logger.warning("  python api.py")
        logger.warning("="*60)
        logger.info("Le pipeline continue normalement sans le calcul du temps de refresh")
        logger.info("="*60)
    
    if results:
        avg_refresh = sum(r["refresh_time"] for r in results) / len(results)
        max_refresh = max(r["refresh_time"] for r in results)
        min_refresh = min(r["refresh_time"] for r in results)
        
        logger.info("="*60)
        logger.info("RESUME DU TEMPS DE REFRESH")
        logger.info("="*60)
        logger.info(f"Nombre de collections testees: {len(results)}")
        logger.info(f"Temps de refresh moyen: {avg_refresh:.3f} secondes")
        logger.info(f"Temps de refresh minimum: {min_refresh:.3f} secondes")
        logger.info(f"Temps de refresh maximum: {max_refresh:.3f} secondes")
        logger.info("="*60)
        
        return {
            "avg_refresh": avg_refresh,
            "min_refresh": min_refresh,
            "max_refresh": max_refresh,
            "collections": results
        }
    else:
        logger.warning("Aucun resultat disponible pour le calcul du temps de refresh")
        return None


def test_refresh_time():
    print("="*60)
    print("TEST DU TEMPS DE REFRESH")
    print("="*60)
    print(f"API URL: {API_URL}")
    print()

    collections = [
        "kpis",
        "fact_achats",
        "ca_par_pays",
        "dim_clients",
        "dim_produits",
        "agg_jour"
    ]
    
    results = []
    
    for collection in collections:
        try:
            print(f"Test de la collection: {collection}")
            response = httpx.get(f"{API_URL}/refresh_time/{collection}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if "error" in data:
                    print(f"  {data['error']}")
                    print()
                    continue
                
                refresh_time = data.get("refresh_time_seconds", 0)
                write_duration = data.get("write_duration_seconds", 0)
                read_duration = data.get("read_duration_seconds", 0)
                record_count = data.get("record_count", 0)
                
                print(f"  Temps de refresh: {refresh_time:.3f} secondes")
                print(f"  Duree ecriture: {write_duration:.3f} secondes")
                print(f"  Duree lecture: {read_duration:.3f} secondes")
                print(f"  Nombre d'enregistrements: {record_count}")
                print(f"  Timestamp ecriture: {data.get('write_timestamp', 'N/A')}")
                print(f"  Timestamp lecture: {data.get('read_timestamp', 'N/A')}")
                print()
                
                results.append({
                    "collection": collection,
                    "refresh_time": refresh_time,
                    "write_duration": write_duration,
                    "read_duration": read_duration,
                    "record_count": record_count
                })
            else:
                print(f"  Erreur HTTP {response.status_code}: {response.text}")
                print()
                
        except httpx.RequestError as e:
            print(f"  Erreur de connexion: {e}")
            print()
        except Exception as e:
            print(f"  Erreur: {e}")
            print()
    
    if results:
        print("="*60)
        print("RESUME")
        print("="*60)
        
        avg_refresh = sum(r["refresh_time"] for r in results) / len(results)
        max_refresh = max(r["refresh_time"] for r in results)
        min_refresh = min(r["refresh_time"] for r in results)
        
        print(f"Nombre de collections testees: {len(results)}")
        print(f"Temps de refresh moyen: {avg_refresh:.3f} secondes")
        print(f"Temps de refresh minimum: {min_refresh:.3f} secondes")
        print(f"Temps de refresh maximum: {max_refresh:.3f} secondes")
        print()
        
        print("Details par collection:")
        for r in sorted(results, key=lambda x: x["refresh_time"]):
            print(f"  {r['collection']}: {r['refresh_time']:.3f}s (ecriture: {r['write_duration']:.3f}s, lecture: {r['read_duration']:.3f}s)")
        
        print("="*60)
    else:
        print("Aucun resultat disponible. Assurez-vous que:")
        print("  1. L'API est lancee (python api.py)")
        print("  2. Le pipeline MongoDB a ete execute (python flows/gold_to_mongodb.py)")
        print("="*60)


if __name__ == "__main__":
    test_refresh_time()
