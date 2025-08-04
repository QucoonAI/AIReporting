import asyncio
from contextlib import asynccontextmanager
from mangum import Mangum
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from app.config.redis import redis_manager
from app.config.dynamodb import get_dynamodb_connection
from app.core.exceptions import setup_exception_handling
from app.core.utils import logger
from app.routes import auth, user, data_source, data_source_update, chat


def create_application(lifespan=None):
    application = FastAPI(lifespan=lifespan)
    application.include_router(user.router)
    application.include_router(auth.router)
    application.include_router(data_source.router)
    application.include_router(data_source_update.router)
    application.include_router(chat.router)
    return application

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info("🔍 Performing DynamoDB health check...")
        dynamodb = get_dynamodb_connection()
        if not dynamodb.health_check():
            logger.error("❌ DynamoDB health check failed")
            logger.error("💡 Ensure infrastructure is deployed")
            raise RuntimeError("DynamoDB health check failed")
        
        logger.info("✅ DynamoDB health check passed")

        # Redis connection with timeout
        logger.info("🔗 Connecting to Redis...")
        redis_connected = await asyncio.wait_for(
            redis_manager.connect(max_retries=3), 
            timeout=30.0
        )
        
        if not redis_connected:
            logger.error("❌ Redis connection failed")
            raise RuntimeError("Redis connection failed and is required")
        else:
            logger.info("✅ Redis connected successfully")
        
        # Add any other startup tasks here
        # startup_tasks.append(initialize_background_tasks())
        
        logger.info("🎉 Application startup completed successfully")
    
    except asyncio.TimeoutError:
        logger.error("❌ Startup timed out")
        raise RuntimeError("Application startup timed out")
    
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        # Cleanup any partially initialized resources
        await cleanup_on_failure()
        raise

    # Application is running
    yield
    
    # Shutdown phase
    logger.info("🔄 Starting application shutdown...")
    
    try:
        # Graceful shutdown with timeout
        shutdown_tasks = [
            redis_manager.disconnect(timeout=10)
        ]
        
        # Wait for all shutdown tasks with overall timeout
        await asyncio.wait_for(
            asyncio.gather(*shutdown_tasks, return_exceptions=True),
            timeout=15.0
        )
        
        logger.info("✅ Application shutdown completed")
        
    except asyncio.TimeoutError:
        logger.warning("⚠️ Shutdown timed out, some resources may not have closed gracefully")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

async def cleanup_on_failure():
    """Cleanup resources when startup fails"""
    try:
        if redis_manager.is_connected:
            await redis_manager.disconnect(timeout=5)
    except Exception as e:
        logger.error(f"Error during startup cleanup: {e}")

app = create_application(lifespan=lifespan)
handler = Mangum(app)

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



