from uvicorn import run

from docling_proxy.config import settings


if __name__ == "__main__":
    run(
        "docling_proxy.app:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        workers=settings.uvicorn_workers,
    )
