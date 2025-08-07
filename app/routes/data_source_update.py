from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Path
from app.services.data_source import DataSourceService
from app.services.data_source_update import DataSourceUpdateService
from app.services.temp_data_source import TempDataSourceService
from app.schemas.data_source_update import (
    ConnectionChangeUpdateRequest,
    MetadataOnlyUpdateRequest,
    ApplyUpdateRequest,
    UpdateInitiationResponse,
    StagedUpdateResponse,
    UpdateApplicationResponse,
    UpdateCancellationResponse,
    PendingUpdatesListResponse,
    SchemaDiffResponse
)
from app.schemas.data_source import DataSourceResponse, DataSourceUpdateRequest
from app.models.user import User
from app.core.dependencies import (
    get_current_user,
    get_data_source_service,
    get_data_source_update_service,
    get_temp_data_source_service,
)
from app.core.utils import logger


router = APIRouter(prefix="/api/v1/data-sources", tags=["Data Source Updates"])

# Update Initiation Endpoints
@router.post("/{data_source_id}/updates/schema-refresh", response_model=UpdateInitiationResponse)
async def initiate_schema_refresh_update(
    data_source_id: int = Path(..., description="ID of the data source to update"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Initiate a schema refresh update by re-extracting schema from the current source.
    The update will be staged for user review before applying.
    """
    try:
        result = await update_service.initiate_schema_refresh_update(
            data_source_id=data_source_id,
            user_id=current_user["user_id"]
        )
        
        return UpdateInitiationResponse(
            message="Schema refresh update initiated. Please review the changes before applying.",
            **result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating schema refresh update for data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate schema refresh update"
        )


@router.post("/{data_source_id}/updates/connection-change", response_model=UpdateInitiationResponse)
async def initiate_connection_change_update(
    request: ConnectionChangeUpdateRequest,
    data_source_id: int = Path(..., description="ID of the data source to update"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Initiate a connection URL change update.
    The new connection will be tested and schema extracted for review.
    """
    try:
        result = await update_service.initiate_connection_change_update(
            data_source_id=data_source_id,
            user_id=current_user["user_id"],
            new_connection_url=request.new_connection_url,
        )
        
        return UpdateInitiationResponse(
            message="Connection change update initiated. Please review the changes before applying.",
            **result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating connection change update for data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate connection change update"
        )


@router.post("/{data_source_id}/updates/file-replace", response_model=UpdateInitiationResponse)
async def initiate_file_replace_update(
    data_source_id: int = Path(..., description="ID of the data source to update"),
    file: UploadFile = File(..., description="New file to replace the current one"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Initiate a file replacement update for file-based data sources.
    The new file will be validated and schema extracted for review.
    """
    try:
        result = await update_service.initiate_file_replace_update(
            data_source_id=data_source_id,
            user_id=current_user["user_id"],
            new_file=file,
        )
        
        return UpdateInitiationResponse(
            message="File replacement update initiated. Please review the changes before applying.",
            **result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating file replace update for data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate file replace update"
        )


# Simple metadata updates (no staging required)
@router.patch("/{data_source_id}/metadata", response_model=DataSourceResponse)
async def update_metadata_only(
    request: MetadataOnlyUpdateRequest,
    data_source_id: int = Path(..., description="ID of the data source to update"),
    data_source_service: DataSourceService = Depends(get_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Update simple metadata like name and LLM description without schema changes.
    This is a direct update that doesn't require staging.
    """
    try:
        # Check ownership
        existing_data_source = await data_source_service.get_data_source_by_id(data_source_id)
        if existing_data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to update this data source"
            )
        
        # Prepare update data
        update_data = DataSourceUpdateRequest()
        
        if request.data_source_name:
            update_data.data_source_name = request.data_source_name
        
        if request.llm_description:
            # For now, we're storing only LLM description in data_source_schema
            update_data.data_source_schema = request.llm_description
        
        # Apply update
        updated_data_source = await data_source_service.update_data_source(
            data_source_id=data_source_id,
            update_data=update_data
        )
        
        return DataSourceResponse.model_validate(updated_data_source)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating metadata for data source {data_source_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data source metadata"
        )


# Staged Update Management Endpoints
@router.get("/updates/{temp_identifier}", response_model=StagedUpdateResponse)
async def get_staged_update(
    temp_identifier: str = Path(..., description="Temporary identifier for the staged update"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed information about a staged update for review.
    Shows current data, proposed changes, and schema differences.
    """
    try:
        staged_update = await update_service.get_staged_update(
            temp_identifier=temp_identifier,
            user_id=current_user["user_id"]
        )
        
        return StagedUpdateResponse(
            message="Staged update retrieved successfully",
            staged_update=staged_update
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting staged update {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve staged update"
        )


@router.get("/updates/{temp_identifier}/diff", response_model=SchemaDiffResponse)
async def get_update_schema_diff(
    temp_identifier: str = Path(..., description="Temporary identifier for the staged update"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed schema differences for a staged update.
    Useful for understanding what will change before applying the update.
    """
    try:
        staged_update = await update_service.get_staged_update(
            temp_identifier=temp_identifier,
            user_id=current_user["user_id"]
        )
        
        schema_diff = staged_update["proposed_changes"].get("schema_diff", {})
        
        # Calculate summary statistics
        summary = {
            "total_changes": (
                len(schema_diff.get("tables_added", [])) +
                len(schema_diff.get("tables_removed", [])) +
                len(schema_diff.get("tables_modified", []))
            ),
            "breaking_changes": len(schema_diff.get("tables_removed", [])) > 0,
            "has_new_tables": len(schema_diff.get("tables_added", [])) > 0,
            "has_modified_tables": len(schema_diff.get("tables_modified", [])) > 0
        }
        
        return SchemaDiffResponse(
            message="Schema diff retrieved successfully",
            temp_identifier=temp_identifier,
            data_source_name=staged_update["current_data"]["data_source_name"],
            schema_diff=schema_diff,
            summary=summary
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting schema diff for update {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve schema diff"
        )


@router.post("/{data_source_id}/apply-update/{temp_identifier}", response_model=UpdateApplicationResponse)
async def apply_staged_update(
    request: ApplyUpdateRequest,
    data_source_id: int = Path(..., description="ID of the data source to update"),
    temp_identifier: str = Path(..., description="Temporary identifier for the staged update"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Apply a staged update to the data source.
    This will make the proposed changes permanent and clean up the staged data.
    """
    try:
        updated_data_source = await update_service.apply_staged_update(
            data_source_id=data_source_id,
            temp_identifier=temp_identifier,
            updated_llm_description=request.updated_llm_description,
            user_id=current_user["user_id"],
        )
        
        return UpdateApplicationResponse(
            message="Update applied successfully",
            data_source=DataSourceResponse.model_validate(updated_data_source)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error applying staged update {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to apply staged update"
        )


@router.delete("/updates/{temp_identifier}", response_model=UpdateCancellationResponse)
async def cancel_staged_update(
    temp_identifier: str = Path(..., description="Temporary identifier for the staged update"),
    update_service: DataSourceUpdateService = Depends(get_data_source_update_service),
    current_user: User = Depends(get_current_user)
):
    """
    Cancel a staged update and clean up temporary data.
    Use this if you decide not to proceed with the proposed changes.
    """
    try:
        success = await update_service.cancel_staged_update(
            temp_identifier=temp_identifier,
            user_id=current_user["user_id"],
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Staged update not found or already processed"
            )
        
        return UpdateCancellationResponse(
            message="Staged update cancelled successfully",
            temp_identifier=temp_identifier
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling staged update {temp_identifier}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel staged update"
        )


# List pending updates
@router.get("/updates", response_model=PendingUpdatesListResponse)
async def list_pending_updates(
    temp_service: TempDataSourceService = Depends(get_temp_data_source_service),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of all pending updates for the current user.
    Shows updates that have been initiated but not yet applied or cancelled.
    """
    try:
        # Get all user extractions and filter for updates
        all_extractions = await temp_service.get_user_extractions(current_user["user_id"])
        
        # Filter for update operations
        pending_updates = []
        for extraction in all_extractions:
            if extraction["temp_identifier"].startswith("update_"):
                # Get more details about this update
                try:
                    detailed_data = await temp_service.get_extraction(
                        extraction["temp_identifier"], 
                        current_user["user_id"]
                    )
                    
                    if detailed_data:
                        update_info = {
                            "temp_identifier": extraction["temp_identifier"],
                            "update_type": detailed_data["extraction_result"].get("update_type", "unknown"),
                            "data_source_id": detailed_data["extraction_result"].get("data_source_id"),
                            "data_source_name": detailed_data["extraction_result"]["current_data"]["data_source_name"],
                            "created_at": extraction["created_at"],
                            "expires_at": extraction["expires_at"],
                            "status": extraction["status"]
                        }
                        pending_updates.append(update_info)
                        
                except Exception as detail_error:
                    logger.warning(f"Could not get details for update {extraction['temp_identifier']}: {detail_error}")
        
        return PendingUpdatesListResponse(
            message=f"Found {len(pending_updates)} pending updates",
            pending_updates=pending_updates,
            total_count=len(pending_updates)
        )
        
    except Exception as e:
        logger.error(f"Error listing pending updates for user {current_user['user_id']}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve pending updates"
        )

