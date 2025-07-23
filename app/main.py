from contextlib import asynccontextmanager
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from config.redis import redis_manager
from config.dynamodb import initialize_dynamodb_tables
from core.exceptions import setup_exception_handling
from routes import auth, user, data_source


def create_application(lifespan=None):
    application = FastAPI(lifespan=lifespan)
    application.include_router(user.router)
    application.include_router(auth.router)
    application.include_router(data_source.router)
    return application

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown"""
    
    # Initialize DynamoDB tables on startup
    initialize_dynamodb_tables()

    # Startup Redis
    await redis_manager.connect()
    print("🚀 Application started successfully")
    yield
    # Shutdown Redis
    await redis_manager.disconnect()
    print("👋 Application shutdown complete")

app = create_application(lifespan=lifespan)

setup_exception_handling(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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


if __name__ == "__main__":
    import uvicorn

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Remove in production
        log_level="info"
    )
