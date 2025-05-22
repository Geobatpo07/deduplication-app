from fastapi import FastAPI
from contextlib import asynccontextmanager
from backend.fastapi_api.api import deduplicated
from services import init_db

# Nouveau handler de cycle de vie
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Démarrage de l’application FastAPI (lifespan)...")
    try:
        init_db.main()  # Met à jour la base DuckDB à partir de SQL Server
    except Exception as e:
        print(f"Erreur lors du chargement initial : {e}")
    yield
    print("Arrêt de l’application FastAPI.")

# Création de l'app avec gestion du cycle de vie
app = FastAPI(
    title="Déduplication API",
    description="API pour exposer les doublons de patients",
    version="1.0.0",
    lifespan=lifespan
)

# Inclusion des routes
app.include_router(deduplicated_data.router, prefix="/api", tags=["Dédoublonnage"])
