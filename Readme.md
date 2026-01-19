# Pipeline ELT - Documentation

## Installation

### 1. Créer l'environnement virtuel

```bash
python -m venv venv
```

### 2. Activer l'environnement virtuel

**Windows (PowerShell):**
```bash
.\venv\Scripts\Activate.ps1
```

**Windows (CMD):**
```bash
venv\Scripts\activate.bat
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Démarrer les services Docker

```bash
docker-compose up -d
```

Cela démarre :
- MinIO (stockage objet)
- PostgreSQL (base de données Prefect)
- Prefect Server (orchestration)

### 5. Configuration

Créer un fichier `.env` à la racine du projet avec :

```
PREFECT_API_URL=http://localhost:4200/api
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=False
```

## Génération des données

Pour générer des données de test :

```bash
python script/generate_data.py
```

Cela crée les fichiers `clients.csv` et `achats.csv` dans `data/sources/`.

## Exécution du pipeline

### Option 1 : Lancer tout le pipeline puis le dashboard

```bash
python run_all.py
```

Cette commande :
1. Exécute le pipeline complet (Bronze → Silver → Gold)
2. Propose de lancer le dashboard automatiquement

### Option 2 : Lancer uniquement l'orchestrateur

```bash
python flows/orchestrate_pipeline.py
```

Exécute les trois couches dans l'ordre : Bronze → Silver → Gold

### Option 3 : Lancer les couches séparément

**Couche Bronze :**
```bash
python flows/bronze_ingestion.py
```

**Couche Silver :**
```bash
python flows/silver_transformation.py
```

**Couche Gold :**
```bash
python flows/gold_agregation.py
```

### Option 4 : Lancer uniquement le dashboard

```bash
streamlit run dashboard.py
```

## URLs disponibles

### Prefect UI
- URL : http://localhost:4200
- Description : Interface pour visualiser les exécutions des flows, les logs et les métriques

### MinIO Console
- URL : http://localhost:9001
- Identifiants : minioadmin / minioadmin
- Description : Interface pour gérer les buckets et visualiser les fichiers stockés

### Dashboard Streamlit
- URL : http://localhost:8501
- Description : Dashboard interactif pour visualiser les données de la couche Gold

## Structure du pipeline

### Couche Bronze
- Source : `data/sources/`
- Destination : Bucket MinIO `bronze`
- Actions : Upload des fichiers sources, copie vers bronze

### Couche Silver
- Source : Bucket MinIO `bronze`
- Destination : Bucket MinIO `silver`
- Actions : Nettoyage, standardisation des dates, normalisation des types, déduplication

### Couche Gold
- Source : Bucket MinIO `silver`
- Destination : Bucket MinIO `gold`
- Actions : Calcul des KPIs, création des tables de dimensions, agrégations temporelles, CA par pays

## Fichiers générés dans Gold

- `fact_achats.csv` : Table de faits (achats + clients)
- `kpis.csv` : Indicateurs clés de performance
- `dim_clients.csv` : Dimension clients
- `dim_produits.csv` : Dimension produits
- `dim_dates.csv` : Dimension dates
- `agg_jour.csv` : Agrégations par jour
- `agg_semaine.csv` : Agrégations par semaine
- `agg_mois.csv` : Agrégations par mois
- `ca_par_pays.csv` : Chiffre d'affaires par pays

## Arrêter les services Docker

```bash
docker-compose down
```

Pour arrêter et supprimer les volumes :

```bash
docker-compose down -v
```
