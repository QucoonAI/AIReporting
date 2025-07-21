from contextlib import asynccontextmanager
from sqlalchemy import text
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from config.database import SessionDep
from config.redis import redis_manager
from core.utils import logger
from routes import auth, user


def create_application(lifespan=None):
    application = FastAPI(lifespan=lifespan)
    application.include_router(user.router)
    application.include_router(auth.router)
    return application

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown"""
    # Startup Redis
    await redis_manager.connect()
    print("🚀 Application started successfully")
    yield
    # Shutdown Redis
    await redis_manager.disconnect()
    print("👋 Application shutdown complete")

app = create_application(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler for better error responses."""
    logger.error(f"Unhandled exception on {request.method} {request.url}.")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.status_code,
                "message": exc.detail,
                "type": "http_error"
            }
        }
    )

@app.get(
    "/health-check",
    status_code=status.HTTP_200_OK,
    summary="Health check",
    description="Check if the service is healthy",
    # include_in_schema=False  # Hide from OpenAPI docs
)
async def health_check():
    """Health check endpoint"""
    return {"message": "Healthy"}

@app.get(
    "/health-check/database",
    status_code=status.HTTP_200_OK,
    summary="Health check (Database)",
    description="Check if the database service is healthy",
    # include_in_schema=False  # Hide from OpenAPI docs
)
async def database_health_check(session: SessionDep): # type: ignore
    """Health check endpoint"""
    try:
        await session.exec(statement=text("SELECT 1"))
        return {"status": "healthy", "db": "connected"}
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}
