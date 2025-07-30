from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
from services.data_source import DataSourceService
from schemas.data_source import (
    DataSourceCreateRequest,
    DataSourceUpdateRequest,
    DataSourceCreateResponse,
    DataSourceUpdateResponse,
    DataSourceDeleteResponse,
    DataSourceResponse,
    DataSourcePaginatedListResponse,
    PaginationMetadata
)
from schemas.enum import DataSourceType
from models.user import User
from core.dependencies import get_current_user, get_data_source_service
from core.utils import logger


router = APIRouter(prefix="/api/v1/data-sources", tags=["Data Sources"])


@router.post("/", response_model=DataSourceCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_data_source(
    data_source_name: str = Form(...),
    data_source_type: DataSourceType = Form(...),
    data_source_url: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Create a new data source.
    
    For file-based data sources (CSV, XLSX, PDF), upload the file.
    For database connections, provide the connection URL.
    """
    try:
        # Validate that either file or URL is provided based on data source type
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

        # Create request object
        if data_source_type in file_based_types:
            # For file-based types, the service will handle S3 upload
            data_source_data = DataSourceCreateRequest(
                data_source_name=data_source_name,
                data_source_type=data_source_type,
                data_source_url="temp://placeholder"  # Temporary placeholder
            )
        else:
            data_source_data = DataSourceCreateRequest(
                data_source_name=data_source_name,
                data_source_type=data_source_type,
                data_source_url=data_source_url
            )

        # Create data source
        created_data_source = await service.create_data_source(
            user_id=current_user["user_id"],
            data_source_data=data_source_data,
            file=file
        )

        return DataSourceCreateResponse(
            message="Data source created successfully",
            data_source=DataSourceResponse.model_validate(created_data_source)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
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


@router.get("/", response_model=DataSourcePaginatedListResponse)
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


