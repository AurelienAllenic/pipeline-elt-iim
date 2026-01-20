"""
Script pour lancer le pipeline ELT complet puis le dashboard.
"""
import subprocess
import sys
from flows.orchestrate_pipeline import elt_pipeline_orchestrator


def main():
    print("="*60)
    print("LANCEMENT DU PIPELINE ELT")
    print("="*60)
    
    try:
        result = elt_pipeline_orchestrator()
        
        print("\n" + "="*60)
        print("PIPELINE ELT TERMINÉ AVEC SUCCÈS")
        print("="*60)
        print(f"Résultats : {result}")
        if isinstance(result, dict) and "mongodb" in result:
            ok = [v for v in result["mongodb"].values() if not str(v).startswith("ERROR")]
            print(f"MongoDB : {len(ok)} collections créées")
        print("="*60)
        
        # Demander si on veut lancer l'API
        api_response = input("\n Voulez-vous lancer l'API FastAPI ? (o/n): ")
        api_thread = None
        
        if api_response.lower() in ['o', 'oui', 'y', 'yes', '']:
            print("\nLancement de l'API FastAPI...")
            print("   L'API sera disponible sur http://localhost:8000")
            
            import threading
            import time
            
            def run_api():
                import uvicorn
                uvicorn.run("api:app", host="0.0.0.0", port=8000, log_level="info")
            
            api_thread = threading.Thread(target=run_api, daemon=True)
            api_thread.start()
            
            # Attendre que l'API démarre
            time.sleep(3)
            print("   API démarrée avec succès")
        
        # Demander si on veut lancer le dashboard
        dashboard_response = input("\n Voulez-vous lancer le dashboard pour visualiser les données ? (o/n): ")
        
        if dashboard_response.lower() in ['o', 'oui', 'y', 'yes', '']:
            if api_response.lower() not in ['o', 'oui', 'y', 'yes', '']:
                print("\n⚠️  ATTENTION : L'API n'est pas lancée !")
                print("   Le dashboard nécessite l'API pour fonctionner.")
                print("   Lancez l'API dans un autre terminal : python api.py")
                input("   Appuyez sur Entrée une fois l'API lancée...")
            
            print("\nLancement du dashboard Streamlit...")
            print("   Le dashboard s'ouvrira dans votre navigateur.")
            print("   Appuyez sur Ctrl+C pour arrêter le dashboard.\n")
            print("="*60 + "\n")
            
            try:
                # Lancer Streamlit
                subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard.py"])
            except KeyboardInterrupt:
                print("\n\nDashboard arrêté.")
            except Exception as e:
                print(f"\nErreur lors du lancement du dashboard : {e}")
                print("   Vous pouvez le lancer manuellement avec : streamlit run dashboard.py")
        else:
            print("\nPour lancer le dashboard plus tard, utilisez :")
            if api_response.lower() not in ['o', 'oui', 'y', 'yes', '']:
                print("   1. python api.py (dans un terminal)")
            print("   2. streamlit run dashboard.py (dans un autre terminal)")
    
    except Exception as e:
        print("\n" + "="*60)
        print("ERREUR LORS DE L'EXÉCUTION DU PIPELINE")
        print("="*60)
        print(f"Erreur : {e}")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
