from fastapi import APIRouter, Depends, Query, HTTPException, status, Path
from typing import Optional, Dict, Any
from services.data_source import DataSourceService
from repositories.data_source import DataSourceRepository
from schemas.data_source import (
    DataSourceCreateRequest, DataSourceCreateResponse, DataSourceUpdateRequest, 
    DataSourceUpdateResponse, DataSourceDeleteResponse, DataSourceResponse,
    DataSourceListResponse, DataSourcePaginatedListResponse, PaginationMetadata
)
from schemas.enum import DataSourceType
from core.dependencies import get_current_user, get_data_source_service, get_data_source_repo
from core.utils import logger


router = APIRouter(prefix="/api/v1/data-sources", tags=["data-sources"])


@router.post(
    "/",
    response_model=DataSourceCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new data source",
    description="Create a new data source for the authenticated user."
)
async def create_data_source(
    data_source_data: DataSourceCreateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    data_source_service: DataSourceService = Depends(get_data_source_service),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourceCreateResponse:
    """
    Create a new data source.
    
    - **data_source_name**: Name of the data source (must be unique per user)
    - **data_source_type**: Type of the data source
    - **data_source_url**: URL of the data source
    
    Returns the created data source information.
    """
    try:
        created_data_source = await data_source_service.create_data_source(
            user_id=current_user["user_id"],
            data_source_data=data_source_data
        )
        data_source = await data_source_repo.get_data_source_by_id(created_data_source.data_source_id)
        return DataSourceCreateResponse(
            message="Data source created successfully",
            data_source=DataSourceResponse.model_validate(data_source)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the data source"
        )


@router.get(
    "/",
    response_model=DataSourceListResponse,
    summary="Get user's data sources",
    description="Get all data sources for the authenticated user."
)
async def get_user_data_sources(
    data_source_type: Optional[DataSourceType] = Query(None, description="Filter by data source type"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourceListResponse:
    """
    Get all data sources for the current user.
    
    - **data_source_type**: Optional filter by data source type
    
    Returns all data sources owned by the authenticated user.
    """
    try:
        data_sources = await data_source_repo.get_user_data_sources(
            user_id=current_user["user_id"],
            data_source_type=data_source_type
        )
        return DataSourceListResponse(
            message="Data sources retrieved successfully",
            data_sources=[DataSourceResponse.model_validate(ds) for ds in data_sources],
            total_count=len(data_sources)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user data sources: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving data sources"
        )


@router.get(
    "/paginated",
    response_model=DataSourcePaginatedListResponse,
    summary="Get paginated user's data sources",
    description="Get paginated data sources for the authenticated user with filtering and sorting."
)
async def get_user_data_sources_paginated(
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    per_page: int = Query(10, ge=1, le=100, description="Number of data sources per page (max 100)"),
    data_source_type: Optional[DataSourceType] = Query(None, description="Filter by data source type"),
    search: Optional[str] = Query(None, description="Search term for data source name"),
    sort_by: str = Query("data_source_created_at", description="Field to sort by"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order (asc or desc)"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourcePaginatedListResponse:
    """
    Get paginated list of user's data sources.
    
    - **page**: Page number (starting from 1)
    - **per_page**: Number of data sources per page (1-100)
    - **data_source_type**: Filter by data source type
    - **search**: Search term to filter by data source name
    - **sort_by**: Field to sort by (data_source_created_at, data_source_updated_at, data_source_name)
    - **sort_order**: Sort order (asc, desc)
    
    Returns paginated list of data sources with metadata.
    """
    try:
        data_sources, total_count = await data_source_repo.get_user_data_sources_paginated(
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting paginated data sources: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving data sources"
        )


@router.get(
    "/{data_source_id}",
    response_model=DataSourceResponse,
    summary="Get data source by ID",
    description="Get a specific data source by ID. Only accessible by the owner."
)
async def get_data_source_by_id(
    data_source_id: int = Path(..., description="ID of the data source to retrieve"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourceResponse:
    """
    Get data source by ID.
    
    - **data_source_id**: ID of the data source to retrieve
    
    Returns the data source information if found and accessible by the user.
    """
    try:
        data_source = await data_source_repo.get_data_source_by_id(data_source_id)
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check if the user owns this data source
        if data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied. You can only access your own data sources."
            )
        
        return DataSourceResponse.model_validate(data_source)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source by ID {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving the data source"
        )


@router.put(
    "/{data_source_id}",
    response_model=DataSourceUpdateResponse,
    summary="Update data source",
    description="Update a data source. Only accessible by the owner."
)
async def update_data_source(
    data_source_id: int = Path(..., description="ID of the data source to update"),
    update_data: DataSourceUpdateRequest = ...,
    current_user: Dict[str, Any] = Depends(get_current_user),
    data_source_service: DataSourceService = Depends(get_data_source_service),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourceUpdateResponse:
    """
    Update data source information.
    
    - **data_source_id**: ID of the data source to update
    - **data_source_name**: Updated name (optional)
    - **data_source_type**: Updated type (optional)
    - **data_source_url**: Updated URL (optional)
    
    Only the owner can update their data source.
    """
    try:
        # Check if data source exists and user owns it
        data_source = await data_source_repo.get_data_source_by_id(data_source_id)
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        if data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied. You can only update your own data sources."
            )
        
        updated_data_source = await data_source_service.update_data_source(
            data_source_id=data_source_id,
            update_data=update_data
        )
        updated_data_source_obj = await data_source_repo.get_data_source_by_id(updated_data_source.data_source_id)
        
        return DataSourceUpdateResponse(
            message="Data source updated successfully",
            data_source=DataSourceResponse.model_validate(updated_data_source_obj)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while updating the data source"
        )


@router.delete(
    "/{data_source_id}",
    response_model=DataSourceDeleteResponse,
    summary="Delete data source",
    description="Delete a data source. Only accessible by the owner."
)
async def delete_data_source(
    data_source_id: int = Path(..., description="ID of the data source to delete"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    data_source_service: DataSourceService = Depends(get_data_source_service),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourceDeleteResponse:
    """
    Delete data source.
    
    - **data_source_id**: ID of the data source to delete
    
    Permanently deletes the data source. Only the owner can delete their data source.
    """
    try:
        # Check if data source exists and user owns it
        data_source = await data_source_repo.get_data_source_by_id(data_source_id)
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        if data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied. You can only delete your own data sources."
            )
        
        message = await data_source_service.delete_data_source(data_source_id)
        return DataSourceDeleteResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the data source"
        )

