from contextlib import asynccontextmanager
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from config.redis import redis_manager
from config.dynamodb import initialize_database, verify_database
from core.exceptions import setup_exception_handling
from core.utils import logger
from routes import auth, user, data_source, chat


def create_application(lifespan=None):
    application = FastAPI(lifespan=lifespan)
    application.include_router(user.router)
    application.include_router(auth.router)
    application.include_router(data_source.router)
    application.include_router(chat.router)
    return application

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info("📊 Initializing DynamoDB tables...")
        db_init_success = await initialize_database()
        
        if not db_init_success:
            logger.error("❌ Database initialization failed")
            raise RuntimeError("Database initialization failed")
        
        # Verify tables are ready
        logger.info("🔍 Verifying database tables...")
        db_verify_success = await verify_database()
        
        if db_verify_success:
            logger.info("✅ Database setup completed successfully")
        else:
            logger.warning("⚠️ Database verification failed")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise

    await redis_manager.connect()
    
    logger.info("🎉 Application startup completed")
    
    yield

    await redis_manager.disconnect()

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



