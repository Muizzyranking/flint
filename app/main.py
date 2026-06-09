from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(version="0.1.0", lifespan=lifespan)


@app.get("/")
def home():
    return {"message": "fastapi is running!"}


@app.get("/health")
def health_check():
    return {"status": "ok"}
