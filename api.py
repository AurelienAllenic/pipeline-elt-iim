import os
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import pandas as pd
from typing import List, Dict, Any

try:
    from flows.config import MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION_PREFIX
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from flows.config import MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION_PREFIX

app = FastAPI(title="ELT Pipeline API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_mongodb_client():
    """Retourne un client MongoDB"""
    if not MONGODB_URI:
        raise ValueError("MONGODB_URI must be set in .env")
    return MongoClient(MONGODB_URI)

def get_collection(collection_name: str):
    """Retourne une collection MongoDB"""
    client = get_mongodb_client()
    db = client[MONGODB_DATABASE]
    full_collection_name = f"{MONGODB_COLLECTION_PREFIX}{collection_name}"
    return db[full_collection_name]

@app.get("/")
def root():
    """Endpoint racine"""
    return {
        "message": "ELT Pipeline API",
        "version": "1.0.0",
        "endpoints": [
            "/kpis",
            "/fact_achats",
            "/dim_clients",
            "/dim_produits",
            "/dim_dates",
            "/agg_jour",
            "/agg_semaine",
            "/agg_mois",
            "/ca_par_pays",
            "/refresh_time/{collection_name}"
        ]
    }

@app.get("/kpis")
def get_kpis():
    """Retourne les KPIs"""
    try:
        collection = get_collection("kpis")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fact_achats")
def get_fact_achats(limit: int = 1000, skip: int = 0):
    """Retourne la table de faits achats"""
    try:
        collection = get_collection("fact_achats")
        data = list(collection.find({}, {"_id": 0}).skip(skip).limit(limit))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dim_clients")
def get_dim_clients():
    """Retourne la dimension clients"""
    try:
        collection = get_collection("dim_clients")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dim_produits")
def get_dim_produits():
    """Retourne la dimension produits"""
    try:
        collection = get_collection("dim_produits")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dim_dates")
def get_dim_dates():
    """Retourne la dimension dates"""
    try:
        collection = get_collection("dim_dates")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agg_jour")
def get_agg_jour():
    """Retourne les agrégations par jour"""
    try:
        collection = get_collection("agg_jour")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agg_semaine")
def get_agg_semaine():
    """Retourne les agrégations par semaine"""
    try:
        collection = get_collection("agg_semaine")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agg_mois")
def get_agg_mois():
    """Retourne les agrégations par mois"""
    try:
        collection = get_collection("agg_mois")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ca_par_pays")
def get_ca_par_pays():
    """Retourne le CA par pays"""
    try:
        collection = get_collection("ca_par_pays")
        data = list(collection.find({}, {"_id": 0}))
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/refresh_time/{collection_name}")
def get_refresh_time(collection_name: str):
    """Calcule le temps de refresh pour une collection"""
    import time
    from datetime import datetime
    
    start_time = time.time()
    timestamp_read_start = datetime.now().isoformat()
    
    try:
        collection = get_collection(collection_name)
        # Faire une lecture test
        count = collection.count_documents({})
        
        end_time = time.time()
        timestamp_read_end = datetime.now().isoformat()
        read_duration = end_time - start_time
        
        # Récupérer les métadonnées d'écriture
        client = get_mongodb_client()
        db = client[MONGODB_DATABASE]
        metadata_collection = db["_refresh_metadata"]
        
        # Le nom de collection dans les métadonnées inclut déjà le préfixe
        full_collection_name = f"{MONGODB_COLLECTION_PREFIX}{collection_name}"
        last_write = metadata_collection.find_one(
            {"collection": full_collection_name},
            sort=[("write_end", -1)]
        )
        
        if last_write:
            write_end = datetime.fromisoformat(last_write["write_end"])
            read_start = datetime.fromisoformat(timestamp_read_start)
            refresh_time = (read_start - write_end).total_seconds()
            
            return {
                "collection": collection_name,
                "full_collection_name": full_collection_name,
                "write_timestamp": last_write["write_end"],
                "read_timestamp": timestamp_read_start,
                "refresh_time_seconds": refresh_time,
                "read_duration_seconds": read_duration,
                "write_duration_seconds": last_write.get("duration_seconds", 0),
                "record_count": count
            }
        else:
            return {
                "collection": collection_name,
                "full_collection_name": full_collection_name,
                "error": "No write metadata found",
                "read_duration_seconds": read_duration,
                "record_count": count
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)