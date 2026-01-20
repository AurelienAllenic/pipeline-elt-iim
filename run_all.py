"""
Script pour lancer le pipeline ELT complet puis le dashboard.
"""
import subprocess
import sys
import threading
import time
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
        api_response = input("\nVoulez-vous lancer l'API FastAPI ? (o/n): ")
        api_running = False
        
        if api_response.lower() in ['o', 'oui', 'y', 'yes', '']:
            print("\nLancement de l'API FastAPI en arrière-plan...")
            print("   L'API sera disponible sur http://localhost:8000")
            
            def run_api():
                import uvicorn
                uvicorn.run("api:app", host="0.0.0.0", port=8000, log_level="info")
            
            api_thread = threading.Thread(target=run_api, daemon=True)
            api_thread.start()
            api_running = True
            
            # Attendre un court instant pour que l'API démarre
            time.sleep(3)
            print("API FastAPI lancée.")
            
            # Calculer le temps de refresh maintenant que l'API est lancée
            print("\n" + "="*60)
            print("CALCUL DU TEMPS DE REFRESH")
            print("="*60)
            try:
                from test_refresh_time import calculate_refresh_time_task
                refresh_result = calculate_refresh_time_task()
                
                if refresh_result:
                    print("\n" + "="*60)
                    print("TEMPS DE REFRESH")
                    print("="*60)
                    print(f"Temps de refresh moyen: {refresh_result.get('avg_refresh', 0):.3f} secondes")
                    print(f"Temps de refresh minimum: {refresh_result.get('min_refresh', 0):.3f} secondes")
                    print(f"Temps de refresh maximum: {refresh_result.get('max_refresh', 0):.3f} secondes")
                    print("="*60)
                else:
                    print("Aucun résultat disponible pour le calcul du temps de refresh")
            except Exception as e:
                print(f"Erreur lors du calcul du temps de refresh : {e}")
        else:
            print("\nL'API FastAPI ne sera pas lancée automatiquement.")
            print("Assurez-vous de la lancer manuellement si le dashboard en a besoin (python api.py).")
        
        # Demander si on veut lancer le dashboard
        dashboard_response = input("\nVoulez-vous lancer le dashboard pour visualiser les données ? (o/n): ")
        
        if dashboard_response.lower() in ['o', 'oui', 'y', 'yes', '']:
            if not api_running:
                print("\nATTENTION : L'API FastAPI n'est pas lancée automatiquement.")
                print("Le dashboard pourrait ne pas fonctionner correctement sans l'API.")
                input("Appuyez sur Entrée pour continuer (ou Ctrl+C pour annuler)...")
                
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
            if not api_running:
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
