from typing import List, Optional, Tuple, Dict, Any
from sqlmodel import select, func, and_, or_
from datetime import datetime, timezone
from app.models.data_source import DataSource
from app.schemas.data_source import DataSourceUpdateRequest
from app.schemas.enum import DataSourceType
from app.config.database import SessionDep
from app.core.utils import logger


class DataSourceRepository:
    """Repository class for handling DataSource database operations."""
    
    def __init__(self, db_session: SessionDep): # type: ignore
        self.session: SessionDep = db_session # type: ignore
    
    async def create_data_source(self, data_source: DataSource) -> DataSource:
        """
        Create a new data source in the database.
        
        Args:
            data_source: DataSource object to create
            
        Returns:
            Created DataSource object with ID
            
        Raises:
            Exception: If creation fails
        """
        try:
            self.session.add(data_source)
            await self.session.flush()
            await self.session.commit()
            await self.session.refresh(data_source)
            return data_source
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating data source: {e}")
            raise
    
    async def update_data_source(
        self, 
        data_source_id: int, 
        update_data: DataSourceUpdateRequest
    ) -> DataSource:
        """
        Update a data source.
        
        Args:
            data_source_id: ID of the data source to update
            update_data: Data to update
            
        Returns:
            Updated DataSource object
            
        Raises:
            Exception: If update fails
        """
        try:
            # Get the existing data source
            data_source = await self.session.get(DataSource, data_source_id)
            if not data_source:
                raise ValueError(f"Data source with ID {data_source_id} not found")
            
            # Update fields if provided
            if update_data.data_source_name is not None:
                data_source.data_source_name = update_data.data_source_name
            if update_data.data_source_type is not None:
                data_source.data_source_type = update_data.data_source_type
            if update_data.data_source_url is not None:
                data_source.data_source_url = update_data.data_source_url
            if update_data.data_source_schema is not None:
                data_source.data_source_schema = update_data.data_source_schema
            
            self.session.add(data_source)
            await self.session.commit()
            await self.session.refresh(data_source)
            
            return data_source
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating data source {data_source_id}: {e}")
            raise

    async def delete_data_source(self, data_source_id: int) -> bool:
        """
        Delete a data source.
        
        Args:
            data_source_id: ID of the data source to delete
            
        Returns:
            True if deleted successfully
            
        Raises:
            Exception: If deletion fails
        """
        try:
            data_source = await self.session.get(DataSource, data_source_id)
            if not data_source:
                return False
            
            data_source.data_source_is_active = False
            self.session.add(data_source)
            await self.session.commit()
            await self.session.refresh(data_source)
            
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting data source {data_source_id}: {e}")
            raise

    async def get_data_source_by_id(self, data_source_id: int) -> Optional[DataSource]:
        """
        Get a data source by its ID.
        
        Args:
            data_source_id: ID of the data source
            
        Returns:
            DataSource object if found, None otherwise
        """
        try:
            return await self.session.get(DataSource, data_source_id)
        except Exception as e:
            logger.error(f"Error getting data source by ID {data_source_id}: {e}")
            raise

    async def get_data_source_by_name(self, user_id: int, name: str) -> Optional[DataSource]:
        """
        Get a data source by user ID and name.
        
        Args:
            user_id: ID of the user
            name: Name of the data source
            
        Returns:
            DataSource object if found, None otherwise
        """
        try:
            statement = select(DataSource).where(
                and_(
                    DataSource.data_source_user_id == user_id,
                    DataSource.data_source_name == name
                )
            )
            result = await self.session.exec(statement)
            return result.first()
        except Exception as e:
            logger.error(f"Error getting data source by name for user {user_id}: {e}")
            raise

    async def get_user_data_sources(self, user_id: int, data_source_type: Optional[DataSourceType] = None) -> List[DataSource]:
        """
        Get all data sources for a user.
        
        Args:
            user_id: ID of the user
            data_source_type: Optional filter by data source type
            
        Returns:
            List of DataSource objects
        """
        try:
            statement = select(DataSource).where(DataSource.data_source_user_id == user_id, DataSource.data_source_is_active == True)

            if data_source_type:
                statement = statement.where(DataSource.data_source_type == data_source_type)
                
            statement = statement.order_by(DataSource.data_source_created_at.desc())
            
            result = await self.session.exec(statement)
            return list(result.all())
        except Exception as e:
            logger.error(f"Error getting user data sources for user {user_id}: {e}")
            raise

    async def get_user_data_sources_paginated(
        self,
        user_id: int,
        page: int = 1,
        per_page: int = 10,
        data_source_type: Optional[DataSourceType] = None,
        search: Optional[str] = None,
        sort_by: str = "data_source_created_at",
        sort_order: str = "desc"
    ) -> Tuple[List[DataSource], int]:
        """
        Get paginated data sources for a user.
        
        Args:
            user_id: ID of the user
            page: Page number (1-based)
            per_page: Number of items per page
            data_source_type: Optional filter by data source type
            search: Optional search term for data source name
            sort_by: Field to sort by
            sort_order: Sort order (asc or desc)
            
        Returns:
            Tuple of (data_sources_list, total_count)
        """
        try:
            # Validate pagination parameters
            if page < 1:
                page = 1
            if per_page < 1:
                per_page = 10
            if per_page > 100:
                per_page = 100
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Build base query
            base_statement = select(DataSource).where(DataSource.data_source_user_id == user_id, DataSource.data_source_is_active == True)
            count_statement = select(func.count(DataSource.data_source_id)).where(
                DataSource.data_source_user_id == user_id, DataSource.data_source_is_active == True
            )
            
            # Apply filters
            filters = [DataSource.data_source_user_id == user_id]
            
            if data_source_type:
                filters.append(DataSource.data_source_type == data_source_type)
            
            if search:
                search_filter = DataSource.data_source_name.ilike(f"%{search}%")
                filters.append(search_filter)
            
            # Apply all filters
            if len(filters) > 1:
                base_statement = base_statement.where(and_(*filters))
                count_statement = count_statement.where(and_(*filters))
            
            # Apply sorting
            sort_column = getattr(DataSource, sort_by, DataSource.data_source_created_at)
            if sort_order.lower() == "desc":
                base_statement = base_statement.order_by(sort_column.desc())
            else:
                base_statement = base_statement.order_by(sort_column.asc())
            
            # Apply pagination
            base_statement = base_statement.offset(offset).limit(per_page)
            
            # Execute queries
            data_sources_result = await self.session.exec(base_statement)
            count_result = await self.session.exec(count_statement)
            
            data_sources = list(data_sources_result.all())
            total_count = count_result.one()
            
            return data_sources, total_count
            
        except Exception as e:
            logger.error(f"Error getting paginated data sources for user {user_id}: {e}")
            raise

    async def get_data_sources_list(
        self,
        page: int = 1,
        per_page: int = 10,
        search: Optional[str] = None,
        data_source_type: Optional[DataSourceType] = None,
        user_id: Optional[int] = None,
        sort_by: str = "data_source_created_at",
        sort_order: str = "desc",
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> Tuple[List[DataSource], int]:
        """
        Get paginated list of data sources with filtering and search (admin function).
        
        Args:
            page: Page number (starting from 1)
            per_page: Number of data sources per page
            search: Search term to filter by name
            data_source_type: Filter by data source type
            user_id: Filter by user ID
            sort_by: Field to sort by
            sort_order: Sort order (asc, desc)
            date_from: Filter data sources created from this date
            date_to: Filter data sources created before this date
            
        Returns:
            Tuple of (data_sources_list, total_count)
        """
        try:
            # Validate pagination parameters
            if page < 1:
                page = 1
            if per_page < 1:
                per_page = 10
            if per_page > 100:
                per_page = 100
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Build base query
            base_statement = select(DataSource).where(DataSource.data_source_is_active == True)
            count_statement = select(func.count(DataSource.data_source_id)).where(DataSource.data_source_is_active == True)

            # Apply filters
            filters = []
            
            # Search filter
            if search:
                search_term = f"%{search.lower()}%"
                search_filters = [
                    func.lower(DataSource.data_source_name).like(search_term),
                    func.lower(DataSource.data_source_url).like(search_term)
                ]
                filters.append(or_(*search_filters))
            
            # Data source type filter
            if data_source_type:
                filters.append(DataSource.data_source_type == data_source_type)
            
            # User ID filter
            if user_id:
                filters.append(DataSource.data_source_user_id == user_id)
            
            # Date range filters
            if date_from:
                filters.append(DataSource.data_source_created_at >= date_from)
            
            if date_to:
                filters.append(DataSource.data_source_created_at <= date_to)
            
            # Apply all filters
            if filters:
                base_statement = base_statement.where(and_(*filters))
                count_statement = count_statement.where(and_(*filters))
            
            # Apply sorting
            sort_field = getattr(DataSource, sort_by, None)
            if sort_field is None:
                sort_field = DataSource.data_source_created_at
            
            if sort_order.lower() == "asc":
                base_statement = base_statement.order_by(sort_field.asc())
            else:
                base_statement = base_statement.order_by(sort_field.desc())
            
            # Apply pagination
            base_statement = base_statement.offset(offset).limit(per_page)
            
            # Execute queries
            data_sources_result = await self.session.exec(base_statement)
            count_result = await self.session.exec(count_statement)
            
            data_sources = list(data_sources_result.all())
            total_count = count_result.one()
            
            return data_sources, total_count
            
        except Exception as e:
            logger.error(f"Error getting data sources list: {e}")
            raise

