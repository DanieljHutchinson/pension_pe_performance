from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.database import init_db
from app.api.routes import router

app = FastAPI(title="Pension PE Performance", version="0.1.0")
app.include_router(router)

static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")


@app.on_event("startup")
def startup():
    init_db()
