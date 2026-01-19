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
        print("="*60)
        
        print("\n" + "="*60)
        print("DASHBOARD")
        print("="*60)
        response = input("\n Voulez-vous lancer le dashboard pour visualiser les données ? (o/n): ")
        
        if response.lower() in ['o', 'oui', 'y', 'yes', '']:
            print("\n📊 Lancement du dashboard Streamlit...")
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
            print("   streamlit run dashboard.py")
    
    except Exception as e:
        print("\n" + "="*60)
        print("ERREUR LORS DE L'EXÉCUTION DU PIPELINE")
        print("="*60)
        print(f"Erreur : {e}")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
