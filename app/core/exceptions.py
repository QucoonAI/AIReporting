from typing import Dict, Any, Union
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy.exc import (
    SQLAlchemyError, IntegrityError, DataError, OperationalError,
    InvalidRequestError, NoResultFound, MultipleResultsFound
)
from pydantic import ValidationError
import redis.exceptions as redis_exceptions
import jwt.exceptions as jwt_exceptions
from .utils import logger


class ErrorResponse:
    """Standard error response format"""
    
    @staticmethod
    def create_error_response(
        message: str,
        error_code: str = None,
        details: Dict[str, Any] = None,
        status_code: int = 500
    ) -> Dict[str, Any]:
        """Create standardized error response"""
        response = {
            "success": False,
            "message": message,
            "status_code": status_code,
            "timestamp": None  # Will be set by middleware if needed
        }
        
        if error_code:
            response["error_code"] = error_code
            
        if details:
            response["details"] = details
            
        return response


class DatabaseExceptionHandler:
    """Handlers for database-related exceptions"""
    
    @staticmethod
    async def handle_sqlalchemy_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        """Handle general SQLAlchemy errors"""
        logger.error(f"Database error: {str(exc)}", exc_info=True)
        
        error_response = ErrorResponse.create_error_response(
            message="A database error occurred. Please try again later.",
            error_code="DATABASE_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response
        )
    
    @staticmethod
    async def handle_integrity_error(request: Request, exc: IntegrityError) -> JSONResponse:
        """Handle database integrity constraint violations"""
        logger.error(f"Database integrity error: {str(exc)}", exc_info=True)
        
        # Parse common integrity errors
        error_message = "Data integrity violation occurred"
        error_code = "INTEGRITY_ERROR"
        
        if "duplicate key" in str(exc).lower() or "unique constraint" in str(exc).lower():
            error_message = "A record with this information already exists"
            error_code = "DUPLICATE_RECORD"
        elif "foreign key" in str(exc).lower():
            error_message = "Referenced record does not exist"
            error_code = "FOREIGN_KEY_ERROR"
        elif "not null" in str(exc).lower():
            error_message = "Required field cannot be empty"
            error_code = "NULL_VALUE_ERROR"
        
        error_response = ErrorResponse.create_error_response(
            message=error_message,
            error_code=error_code,
            status_code=status.HTTP_400_BAD_REQUEST
        )
        
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=error_response
        )
    
    @staticmethod
    async def handle_data_error(request: Request, exc: DataError) -> JSONResponse:
        """Handle database data format errors"""
        logger.error(f"Database data error: {str(exc)}", exc_info=True)
        
        error_response = ErrorResponse.create_error_response(
            message="Invalid data format provided",
            error_code="DATA_FORMAT_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST
        )
        
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=error_response
        )
    
    @staticmethod
    async def handle_operational_error(request: Request, exc: OperationalError) -> JSONResponse:
        """Handle database operational errors (connection issues, etc.)"""
        logger.error(f"Database operational error: {str(exc)}", exc_info=True)
        
        error_response = ErrorResponse.create_error_response(
            message="Database service is temporarily unavailable. Please try again later.",
            error_code="DATABASE_UNAVAILABLE",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )
        
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=error_response
        )
    
    @staticmethod
    async def handle_no_result_found(request: Request, exc: NoResultFound) -> JSONResponse:
        """Handle cases where expected database record is not found"""
        logger.warning(f"No result found: {str(exc)}")
        
        error_response = ErrorResponse.create_error_response(
            message="Requested resource not found",
            error_code="RESOURCE_NOT_FOUND",
            status_code=status.HTTP_404_NOT_FOUND
        )
        
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=error_response
        )


class RedisExceptionHandler:
    """Handlers for Redis-related exceptions"""
    
    @staticmethod
    async def handle_redis_connection_error(request: Request, exc: redis_exceptions.ConnectionError) -> JSONResponse:
        """Handle Redis connection errors"""
        logger.error(f"Redis connection error: {str(exc)}", exc_info=True)
        
        error_response = ErrorResponse.create_error_response(
            message="Cache service is temporarily unavailable. Please try again later.",
            error_code="CACHE_UNAVAILABLE",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )
        
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=error_response
        )
    
    @staticmethod
    async def handle_redis_timeout_error(request: Request, exc: redis_exceptions.TimeoutError) -> JSONResponse:
        """Handle Redis timeout errors"""
        logger.error(f"Redis timeout error: {str(exc)}", exc_info=True)
        
        error_response = ErrorResponse.create_error_response(
            message="Cache service request timed out. Please try again.",
            error_code="CACHE_TIMEOUT",
            status_code=status.HTTP_504_GATEWAY_TIMEOUT
        )
        
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content=error_response
        )


class AuthenticationExceptionHandler:
    """Handlers for authentication and authorization exceptions"""
    
    @staticmethod
    async def handle_jwt_decode_error(request: Request, exc: jwt_exceptions.DecodeError) -> JSONResponse:
        """Handle JWT decode errors"""
        logger.warning(f"JWT decode error: {str(exc)}")
        
        error_response = ErrorResponse.create_error_response(
            message="Invalid authentication token",
            error_code="INVALID_TOKEN",
            status_code=status.HTTP_401_UNAUTHORIZED
        )
        
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content=error_response,
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    @staticmethod
    async def handle_jwt_expired_error(request: Request, exc: jwt_exceptions.ExpiredSignatureError) -> JSONResponse:
        """Handle JWT expired token errors"""
        logger.warning(f"JWT expired error: {str(exc)}")
        
        error_response = ErrorResponse.create_error_response(
            message="Authentication token has expired",
            error_code="TOKEN_EXPIRED",
            status_code=status.HTTP_401_UNAUTHORIZED
        )
        
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content=error_response,
            headers={"WWW-Authenticate": "Bearer"}
        )


class ValidationExceptionHandler:
    """Handlers for validation errors"""
    
    @staticmethod
    async def handle_request_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        """Handle FastAPI request validation errors"""
        logger.warning(f"Request validation error: {exc.errors()}")
        
        # Format validation errors for better readability
        formatted_errors = []
        for error in exc.errors():
            field = " -> ".join(str(loc) for loc in error["loc"])
            formatted_errors.append({
                "field": field,
                "message": error["msg"],
                "type": error["type"]
            })
        
        error_response = ErrorResponse.create_error_response(
            message="Request validation failed",
            error_code="VALIDATION_ERROR",
            details={"validation_errors": formatted_errors},
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )
        
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=error_response
        )
    
    @staticmethod
    async def handle_pydantic_validation_error(request: Request, exc: ValidationError) -> JSONResponse:
        """Handle Pydantic validation errors"""
        logger.warning(f"Pydantic validation error: {exc.errors()}")
        
        formatted_errors = []
        for error in exc.errors():
            field = " -> ".join(str(loc) for loc in error["loc"])
            formatted_errors.append({
                "field": field,
                "message": error["msg"],
                "type": error["type"]
            })
        
        error_response = ErrorResponse.create_error_response(
            message="Data validation failed",
            error_code="DATA_VALIDATION_ERROR",
            details={"validation_errors": formatted_errors},
            status_code=status.HTTP_400_BAD_REQUEST
        )
        
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=error_response
        )


class HTTPExceptionHandler:
    """Handlers for HTTP exceptions"""
    
    @staticmethod
    async def handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        """Handle FastAPI HTTP exceptions"""
        logger.info(f"HTTP exception: {exc.status_code} - {exc.detail}")
        
        error_response = ErrorResponse.create_error_response(
            message=exc.detail,
            error_code="HTTP_ERROR",
            status_code=exc.status_code
        )
        
        # Add any additional headers from the exception
        headers = getattr(exc, 'headers', None)
        
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response,
            headers=headers
        )
    
    @staticmethod
    async def handle_starlette_http_exception(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        """Handle Starlette HTTP exceptions"""
        logger.info(f"Starlette HTTP exception: {exc.status_code} - {exc.detail}")
        
        error_response = ErrorResponse.create_error_response(
            message=exc.detail,
            error_code="HTTP_ERROR",
            status_code=exc.status_code
        )
        
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response
        )


class GeneralExceptionHandler:
    """Handlers for general exceptions"""
    
    @staticmethod
    async def handle_general_exception(request: Request, exc: Exception) -> JSONResponse:
        """Handle unexpected exceptions"""
        logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
        
        error_response = ErrorResponse.create_error_response(
            message="An unexpected error occurred. Please try again later.",
            error_code="INTERNAL_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response
        )


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register all exception handlers with the FastAPI application.
    
    Args:
        app: FastAPI application instance
    """
    
    # Database exceptions
    app.add_exception_handler(SQLAlchemyError, DatabaseExceptionHandler.handle_sqlalchemy_error)
    app.add_exception_handler(IntegrityError, DatabaseExceptionHandler.handle_integrity_error)
    app.add_exception_handler(DataError, DatabaseExceptionHandler.handle_data_error)
    app.add_exception_handler(OperationalError, DatabaseExceptionHandler.handle_operational_error)
    app.add_exception_handler(NoResultFound, DatabaseExceptionHandler.handle_no_result_found)
    
    # Redis exceptions
    app.add_exception_handler(redis_exceptions.ConnectionError, RedisExceptionHandler.handle_redis_connection_error)
    app.add_exception_handler(redis_exceptions.TimeoutError, RedisExceptionHandler.handle_redis_timeout_error)
    
    # Authentication exceptions
    app.add_exception_handler(jwt_exceptions.DecodeError, AuthenticationExceptionHandler.handle_jwt_decode_error)
    app.add_exception_handler(jwt_exceptions.ExpiredSignatureError, AuthenticationExceptionHandler.handle_jwt_expired_error)
    
    # Validation exceptions
    app.add_exception_handler(RequestValidationError, ValidationExceptionHandler.handle_request_validation_error)
    app.add_exception_handler(ValidationError, ValidationExceptionHandler.handle_pydantic_validation_error)
    
    # HTTP exceptions
    app.add_exception_handler(HTTPException, HTTPExceptionHandler.handle_http_exception)
    app.add_exception_handler(StarletteHTTPException, HTTPExceptionHandler.handle_starlette_http_exception)
    
    # General exceptions (this should be last)
    app.add_exception_handler(Exception, GeneralExceptionHandler.handle_general_exception)
    
    logger.info("Exception handlers registered successfully")


# Custom exceptions for the application
class UserNotFoundError(Exception):
    """Raised when a user is not found"""
    def __init__(self, user_id: Union[int, str]):
        self.user_id = user_id
        super().__init__(f"User with ID {user_id} not found")


class DataSourceNotFoundError(Exception):
    """Raised when a data source is not found"""
    def __init__(self, data_source_id: int):
        self.data_source_id = data_source_id
        super().__init__(f"Data source with ID {data_source_id} not found")


class DataSourceLimitExceededError(Exception):
    """Raised when user exceeds data source limit"""
    def __init__(self, limit: int):
        self.limit = limit
        super().__init__(f"Maximum limit of {limit} data sources per user reached")


class InvalidOTPError(Exception):
    """Raised when OTP is invalid or expired"""
    def __init__(self, message: str = "Invalid or expired OTP"):
        super().__init__(message)


class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded"""
    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(message)


# Custom exception handlers for application-specific exceptions
class CustomExceptionHandler:
    """Handlers for custom application exceptions"""
    
    @staticmethod
    async def handle_user_not_found(request: Request, exc: UserNotFoundError) -> JSONResponse:
        """Handle user not found errors"""
        logger.warning(f"User not found: {exc.user_id}")
        
        error_response = ErrorResponse.create_error_response(
            message=str(exc),
            error_code="USER_NOT_FOUND",
            status_code=status.HTTP_404_NOT_FOUND
        )
        
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=error_response
        )
    
    @staticmethod
    async def handle_data_source_not_found(request: Request, exc: DataSourceNotFoundError) -> JSONResponse:
        """Handle data source not found errors"""
        logger.warning(f"Data source not found: {exc.data_source_id}")
        
        error_response = ErrorResponse.create_error_response(
            message=str(exc),
            error_code="DATA_SOURCE_NOT_FOUND",
            status_code=status.HTTP_404_NOT_FOUND
        )
        
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=error_response
        )
    
    @staticmethod
    async def handle_data_source_limit_exceeded(request: Request, exc: DataSourceLimitExceededError) -> JSONResponse:
        """Handle data source limit exceeded errors"""
        logger.warning(f"Data source limit exceeded: {exc.limit}")
        
        error_response = ErrorResponse.create_error_response(
            message=str(exc),
            error_code="DATA_SOURCE_LIMIT_EXCEEDED",
            status_code=status.HTTP_400_BAD_REQUEST
        )
        
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=error_response
        )
    
    @staticmethod
    async def handle_invalid_otp(request: Request, exc: InvalidOTPError) -> JSONResponse:
        """Handle invalid OTP errors"""
        logger.warning(f"Invalid OTP: {str(exc)}")
        
        error_response = ErrorResponse.create_error_response(
            message=str(exc),
            error_code="INVALID_OTP",
            status_code=status.HTTP_400_BAD_REQUEST
        )
        
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=error_response
        )
    
    @staticmethod
    async def handle_rate_limit_exceeded(request: Request, exc: RateLimitExceededError) -> JSONResponse:
        """Handle rate limit exceeded errors"""
        logger.warning(f"Rate limit exceeded: {str(exc)}")
        
        error_response = ErrorResponse.create_error_response(
            message=str(exc),
            error_code="RATE_LIMIT_EXCEEDED",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS
        )
        
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content=error_response,
            headers={"Retry-After": "3600"}  # Suggest retry after 1 hour
        )


def register_custom_exception_handlers(app: FastAPI) -> None:
    """
    Register custom application exception handlers.
    
    Args:
        app: FastAPI application instance
    """
    app.add_exception_handler(UserNotFoundError, CustomExceptionHandler.handle_user_not_found)
    app.add_exception_handler(DataSourceNotFoundError, CustomExceptionHandler.handle_data_source_not_found)
    app.add_exception_handler(DataSourceLimitExceededError, CustomExceptionHandler.handle_data_source_limit_exceeded)
    app.add_exception_handler(InvalidOTPError, CustomExceptionHandler.handle_invalid_otp)
    app.add_exception_handler(RateLimitExceededError, CustomExceptionHandler.handle_rate_limit_exceeded)
    
    logger.info("Custom exception handlers registered successfully")


def setup_exception_handling(app: FastAPI) -> None:
    """
    Complete setup of exception handling for the application.
    
    Args:
        app: FastAPI application instance
    """
    register_exception_handlers(app)
    register_custom_exception_handlers(app)
    logger.info("All exception handlers setup completed")




