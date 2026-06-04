import os

from fastapi import FastAPI, Request

from app.logging_config import get_logger, get_or_create_correlation_id, setup_logging
from app.repository import init_root_doc
from app.token_manager import TokenManager

from app.api import email_routes, fcm_routes, health_routes, push_routes, thread_routes, token_routes, watch_routes

setup_logging()
logger = get_logger()

app = FastAPI(title="CalendAI Python Microservice")
token_manager = TokenManager()


@app.on_event("startup")
async def on_startup():
    init_root_doc()


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    cid = get_or_create_correlation_id(request.headers.get("X-Request-ID"))
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


app.include_router(health_routes.router)
app.include_router(fcm_routes.router)
app.include_router(email_routes.create_router(token_manager))
app.include_router(push_routes.create_router(token_manager))
app.include_router(token_routes.create_router(token_manager))
app.include_router(watch_routes.create_router(token_manager))
app.include_router(thread_routes.create_router(token_manager))


if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv

    load_dotenv()
    port = int(os.getenv("PORT", "5000"))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)
