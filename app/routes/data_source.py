from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form, Path
from app.services.data_source import DataSourceService
from app.services.redis_managers.data_source import TempDataSourceService
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
from app.core.dependencies import get_current_user, get_data_source_service, get_temp_data_source_service
from app.core.utils import logger


router = APIRouter(prefix="/api/v1/data-sources", tags=["Data Sources"])

@router.post("/upload-extract", response_model=DataSourceSchemaExtractionResponse, status_code=status.HTTP_200_OK)
async def upload_and_extract_schema(
    data_source_name: str = Form(...),
    data_source_type: DataSourceType = Form(...),
    data_source_url: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Upload file and extract schema without saving to database or S3.
    Extraction result is cached in Redis for later creation.
    
    Returns the extracted schema with temp_identifier for user review.
    """
    try:
        # Validate inputs
        file_based_types = [DataSourceType.CSV, DataSourceType.XLSX, DataSourceType.PDF]
        
        if data_source_type in file_based_types:
            if not file:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File is required for {data_source_type.value} data source type"
                )
            if data_source_url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"URL should not be provided for file-based data source type {data_source_type.value}"
                )
        else:
            if not data_source_url:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"URL is required for {data_source_type.value} data source type"
                )
            if file:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File should not be provided for URL-based data source type {data_source_type.value}"
                )

        # Extract schema using the service
        extraction_result = await service.upload_and_extract_schema(
            user_id=current_user["user_id"],
            data_source_name=data_source_name,
            data_source_type=data_source_type.value,
            data_source_url=data_source_url,
            file=file
        )

        return DataSourceSchemaExtractionResponse(
            message="Schema extracted successfully. Please review and modify the description before creating the data source.",
            **extraction_result
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
async def create_data_source_with_schema(
    request: DataSourceCreateWithSchemaRequest,
    temp_identifier: str = Path(..., description="Temporary identifier from schema extraction"),
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Create a data source using previously extracted schema.
    
    The temp_identifier should be obtained from the /upload-extract endpoint.
    For file-based sources, this will upload the file to S3 during creation.
    """
    try:
        # Create data source using the enhanced service method
        created_data_source = await service.create_data_source_with_cached_extraction(
            user_id=current_user["user_id"],
            temp_identifier=temp_identifier,
            llm_description=request.llm_description,
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
async def update_data_source(
    data_source_id: int,
    update_data: DataSourceUpdateRequest,
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """Update an existing data source"""
    try:
        # Check if data source belongs to current user
        existing_data_source = await service.get_data_source_by_id(data_source_id)
        if existing_data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to update this data source"
            )

        updated_data_source = await service.update_data_source(
            data_source_id=data_source_id,
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
async def get_data_source(
    data_source_id: int,
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """Get a data source by ID"""
    try:
        data_source = await service.get_data_source_by_id(data_source_id)
        
        # Check if data source belongs to current user
        if data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to access this data source"
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

@router.get("", response_model=DataSourcePaginatedListResponse)
async def list_user_data_sources(
    page: int = 1,
    per_page: int = 10,
    data_source_type: Optional[DataSourceType] = None,
    search: Optional[str] = None,
    sort_by: str = "data_source_created_at",
    sort_order: str = "desc",
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """Get paginated list of user's data sources"""
    try:
        data_sources, total_count = await service.get_user_data_sources_paginated(
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
        logger.error(f"Error listing data sources for user {current_user["user_id"]}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve data sources"
        )

@router.delete("/{data_source_id}", response_model=DataSourceDeleteResponse)
async def delete_data_source(
    data_source_id: int,
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """Delete a data source"""
    try:
        # Check if data source belongs to current user
        existing_data_source = await service.get_data_source_by_id(data_source_id)
        if existing_data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to delete this data source"
            )

        message = await service.delete_data_source(data_source_id)
        
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
async def list_pending_extractions(
    temp_service: TempDataSourceService = Depends(get_temp_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of pending schema extractions that haven't been created yet.
    (These are temporary extractions stored in Redis waiting for user approval).
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
async def get_pending_extraction(
    temp_identifier: str = Path(..., description="Temporary identifier from schema extraction"),
    temp_service: TempDataSourceService = Depends(get_temp_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed information about a specific pending extraction.
    Useful for reviewing the extracted schema before creating the data source.
    """
    try:
        extraction_data = await temp_service.get_extraction(temp_identifier, current_user["user_id"])
        
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
async def delete_pending_extraction(
    temp_identifier: str = Path(..., description="Temporary identifier from schema extraction"),
    temp_service: TempDataSourceService = Depends(get_temp_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a pending extraction if the user decides not to create the data source.
    This cleans up temporary data from Redis.
    """
    try:
        success = await temp_service.delete_extraction(temp_identifier, current_user["user_id"])
        
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

