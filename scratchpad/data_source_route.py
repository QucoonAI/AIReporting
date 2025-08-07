from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form, Path
from app.services.enhanced_data_source_service import EnhancedDataSourceService
from app.services.enhanced_temp_data_service import EnhancedTempDataService
from app.services.streaming_file_service import StreamingFileService
from app.schemas.data_source import (
    DataSourceUpdateRequest,
    DataSourceCreateResponse,
    DataSourceUpdateResponse,
    DataSourceDeleteResponse,
    DataSourceResponse,
    DataSourcePaginatedListResponse,
    PaginationMetadata,
    DataSourceSchemaExtractionResponse,
    DataSourceCreateWithSchemaRequest,
    PendingExtractionResponse,
    PendingExtractionListResponse
)
from app.schemas.enum import DataSourceType
from app.models.user import User
from app.core.dependencies import get_current_user
from app.core.utils import logger
from app.services.validation_middleware import (
    DataSourceValidationRequest,
    require_ownership,
    validate_user_limits_dependency
)

router = APIRouter(prefix="/api/v1/data-sources", tags=["Data Sources"])

@router.post("/upload-extract", response_model=DataSourceSchemaExtractionResponse, status_code=status.HTTP_200_OK)
async def upload_and_extract_schema_enhanced(
    payload: DataSourceValidationRequest = Depends(),
    file: Optional[UploadFile] = File(None),
    data_source_service: EnhancedDataSourceService = Depends(),
    temp_service: EnhancedTempDataService = Depends(),
    _: bool = Depends(validate_user_limits_dependency),  # Pre-validate user limits
    current_user: User = Depends(get_current_user)
):
    """
    Enhanced upload and extract schema with streaming file processing and validation
    """
    try:
        # Validate input combination
        file_based_types = [DataSourceType.CSV, DataSourceType.XLSX, DataSourceType.PDF]
        
        if DataSourceType(payload.data_source_type) in file_based_types:
            if not file:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File is required for {payload.data_source_type} data source type"
                )
            if payload.data_source_url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"URL should not be provided for file-based data source type {payload.data_source_type}"
                )
        else:
            if not payload.data_source_url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"URL is required for {payload.data_source_type} data source type"
                )
            if file:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File should not be provided for URL-based data source type {payload.data_source_type}"
                )

        # Extract schema using enhanced service with streaming
        extraction_result = await data_source_service.upload_and_extract_schema_streaming(
            user_id=current_user["user_id"],
            data_source_name=payload.data_source_name,
            data_source_type=payload.data_source_type,
            data_source_url=payload.data_source_url,
            file=file
        )
        
        # Store in temporary cache with file reference
        temp_file_path = None
        if file and DataSourceType(payload.data_source_type) in file_based_types:
            # File path would be available from the streaming service context
            # For this example, we'll use the enhanced temp service
            pass
        
        temp_identifier = await temp_service.store_extraction_with_file_reference(
            user_id=current_user["user_id"],
            data_source_name=payload.data_source_name,
            extraction_result=extraction_result,
            file_path=temp_file_path
        )
        
        # Add temp_identifier to the response
        response_data = {
            **extraction_result,
            "temp_identifier": temp_identifier
        }

        return DataSourceSchemaExtractionResponse(
            message="Schema extracted successfully. Please review and modify the description before creating the data source.",
            **response_data
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extracting schema: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to extract schema from data source"
        )

@router.post("/create/{temp_identifier}", response_model=DataSourceCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_data_source_with_transaction(
    payload: DataSourceCreateWithSchemaRequest,
    temp_identifier: str = Path(..., description="Temporary identifier from schema extraction"),
    data_source_service: EnhancedDataSourceService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Create data source using transaction manager for atomic operations
    """
    try:
        # Create data source using enhanced service with transaction
        created_data_source = await data_source_service.create_data_source_with_transaction(
            user_id=current_user["user_id"],
            temp_identifier=temp_identifier,
            updated_llm_description=payload.updated_llm_description,
        )

        return DataSourceCreateResponse(
            message="Data source created successfully",
            data_source=DataSourceResponse.model_validate(created_data_source)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source with temp_identifier {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create data source"
        )

@router.put("/{data_source_id}", response_model=DataSourceUpdateResponse)
async def update_data_source_enhanced(
    data_source_id: int,
    update_data: DataSourceUpdateRequest,
    data_source_service: EnhancedDataSourceService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Update data source with distributed locking and validation
    """
    try:
        updated_data_source = await data_source_service.update_data_source_with_validation(
            data_source_id=data_source_id,
            user_id=current_user["user_id"],
            update_data=update_data
        )

        return DataSourceUpdateResponse(
            message="Data source updated successfully",
            data_source=DataSourceResponse.model_validate(updated_data_source)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data source"
        )

@router.get("/{data_source_id}", response_model=DataSourceResponse)
async def get_data_source_enhanced(
    data_source_id: int,
    data_source_service: EnhancedDataSourceService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Get data source with ownership validation and caching
    """
    try:
        data_source = await data_source_service.get_data_source_by_id_with_validation(
            data_source_id, 
            current_user["user_id"]
        )

        return DataSourceResponse.model_validate(data_source)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve data source"
        )

@router.get("/", response_model=DataSourcePaginatedListResponse)
async def list_user_data_sources_enhanced(
    page: int = 1,
    per_page: int = 10,
    data_source_type: Optional[DataSourceType] = None,
    search: Optional[str] = None,
    sort_by: str = "data_source_created_at",
    sort_order: str = "desc",
    data_source_service: EnhancedDataSourceService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Get paginated list of user's data sources with input sanitization
    """
    try:
        data_sources, total_count = await data_source_service.get_user_data_sources_paginated(
            user_id=current_user["user_id"],
            page=page,
            per_page=per_page,
            data_source_type=data_source_type,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order
        )

        # Calculate pagination metadata
        total_pages = (total_count + per_page - 1) // per_page
        has_next = page < total_pages
        has_prev = page > 1

        pagination = PaginationMetadata(
            page=page,
            per_page=per_page,
            total=total_count,
            total_pages=total_pages,
            has_next=has_next,
            has_prev=has_prev
        )

        return DataSourcePaginatedListResponse(
            message="Data sources retrieved successfully",
            data_sources=[DataSourceResponse.model_validate(ds) for ds in data_sources],
            pagination=pagination
        )

    except Exception as e:
        logger.error(f"Error listing data sources for user {current_user['user_id']}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve data sources"
        )

@router.delete("/{data_source_id}", response_model=DataSourceDeleteResponse)
async def delete_data_source_enhanced(
    data_source_id: int,
    data_source_service: EnhancedDataSourceService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Delete data source with proper cleanup using transaction
    """
    try:
        message = await data_source_service.delete_data_source_with_cleanup(
            data_source_id, 
            current_user["user_id"]
        )
        
        return DataSourceDeleteResponse(message=message)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete data source"
        )

@router.get("/pending", response_model=PendingExtractionListResponse)
async def list_pending_extractions_enhanced(
    temp_service: EnhancedTempDataService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of pending schema extractions with cleanup
    """
    try:
        # Clean up expired extractions first
        await temp_service.cleanup_expired_extractions(current_user["user_id"])
        
        # Get current pending extractions
        pending_extractions = await temp_service.get_user_extractions(current_user["user_id"])
        
        return PendingExtractionListResponse(
            message=f"Found {len(pending_extractions)} pending extractions",
            pending_extractions=pending_extractions,
            total_count=len(pending_extractions)
        )

    except Exception as e:
        logger.error(f"Error listing pending extractions for user {current_user['user_id']}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve pending extractions"
        )

@router.get("/pending/{temp_identifier}", response_model=PendingExtractionResponse)
async def get_pending_extraction_enhanced(
    temp_identifier: str = Path(..., description="Temporary identifier from schema extraction"),
    temp_service: EnhancedTempDataService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed information about a specific pending extraction
    """
    try:
        extraction_data = await temp_service.get_extraction_with_file_content(
            temp_identifier, 
            current_user["user_id"],
            include_file_content=False  # Don't include file content for preview
        )
        
        if not extraction_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pending extraction not found or expired"
            )

        return PendingExtractionResponse(
            message="Pending extraction retrieved successfully",
            temp_identifier=temp_identifier,
            extraction_result=extraction_data["extraction_result"],
            created_at=extraction_data["created_at"],
            expires_at=extraction_data["expires_at"],
            has_file=extraction_data.get("has_file", False)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pending extraction {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve pending extraction"
        )

@router.delete("/pending/{temp_identifier}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pending_extraction_enhanced(
    temp_identifier: str = Path(..., description="Temporary identifier from schema extraction"),
    temp_service: EnhancedTempDataService = Depends(),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a pending extraction with proper cleanup
    """
    try:
        success = await temp_service.cleanup_extraction_and_files(
            temp_identifier, 
            current_user["user_id"]
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pending extraction not found or already expired"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting pending extraction {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete pending extraction"
        )

