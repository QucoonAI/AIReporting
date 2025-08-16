from typing import List, Optional, Tuple
from sqlmodel import select, func, and_, or_
from datetime import datetime
from app.models.data_source import DataSource
from app.schemas.data_source import DataSourceUpdateRequest
from app.schemas.enum import DataSourceType
from app.config.database import SessionDep
from app.core.utils import logger


MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 10


class DataSourceRepository:
    """Repository for handling DataSource database operations."""

    def __init__(self, db_session: SessionDep):  # type: ignore
        self.session: SessionDep = db_session  # type: ignore

    async def create(self, data_source: DataSource) -> DataSource:
        """Create a new data source."""
        try:
            self.session.add(data_source)
            await self.session.commit()
            await self.session.refresh(data_source)
            return data_source
        except Exception:
            await self.session.rollback()
            logger.exception("Error creating data source")
            raise

    async def update(self, data_source_id: int, update_data: DataSourceUpdateRequest) -> DataSource:
        """Update a data source."""
        try:
            data_source = await self.session.get(DataSource, data_source_id)
            if not data_source:
                raise ValueError(f"Data source {data_source_id} not found")

            for field, value in update_data.model_dump(exclude_unset=True).items():
                setattr(data_source, field, value)

            await self.session.commit()
            await self.session.refresh(data_source)
            return data_source
        except Exception:
            await self.session.rollback()
            logger.exception(f"Error updating data source {data_source_id}")
            raise

    async def soft_delete(self, data_source_id: int) -> bool:
        """Soft delete a data source (mark as inactive)."""
        try:
            data_source = await self.session.get(DataSource, data_source_id)
            if not data_source:
                return False

            data_source.data_source_is_active = False

            await self.session.commit()
            return True
        except Exception:
            await self.session.rollback()
            logger.exception(f"Error deleting data source {data_source_id}")
            raise

    async def get_by_id(self, data_source_id: int) -> Optional[DataSource]:
        """Retrieve a data source by its ID."""
        try:
            return await self.session.get(DataSource, data_source_id)
        except Exception:
            logger.exception(f"Error fetching data source by ID {data_source_id}")
            raise

    async def get_by_name(self, user_id: int, name: str) -> Optional[DataSource]:
        """Retrieve a data source by user ID and name."""
        try:
            statement = select(DataSource).where(
                and_(
                    DataSource.data_source_user_id == user_id,
                    DataSource.data_source_name == name
                )
            )
            return (await self.session.exec(statement)).first()
        except Exception:
            logger.exception(f"Error fetching data source by name for user {user_id}")
            raise

    async def get_user_sources(
        self, user_id: int, data_source_type: Optional[DataSourceType] = None
    ) -> List[DataSource]:
        """Retrieve all active data sources for a user."""
        try:
            filters = [
                DataSource.data_source_user_id == user_id,
                DataSource.data_source_is_active.is_(True)
            ]
            if data_source_type:
                filters.append(DataSource.data_source_type == data_source_type)

            statement = select(DataSource).where(and_(*filters)).order_by(
                DataSource.data_source_created_at.desc()
            )

            return list((await self.session.exec(statement)).all())
        except Exception:
            logger.exception(f"Error fetching user data sources for user {user_id}")
            raise

    async def get_user_sources_paginated(
        self,
        user_id: int,
        page: int = 1,
        per_page: int = DEFAULT_PAGE_SIZE,
        data_source_type: Optional[DataSourceType] = None,
        search: Optional[str] = None,
        sort_by: str = "data_source_created_at",
        sort_order: str = "desc"
    ) -> Tuple[List[DataSource], int]:
        """Retrieve paginated active data sources for a user."""
        try:
            page, per_page = self._validate_pagination(page, per_page)

            filters = [
                DataSource.data_source_user_id == user_id,
                DataSource.data_source_is_active.is_(True)
            ]
            if data_source_type:
                filters.append(DataSource.data_source_type == data_source_type)
            if search:
                filters.append(DataSource.data_source_name.ilike(f"%{search}%"))

            sort_column = getattr(DataSource, sort_by, DataSource.data_source_created_at)
            sort_order_func = sort_column.desc if sort_order.lower() == "desc" else sort_column.asc

            base_query = select(DataSource).where(and_(*filters)).order_by(sort_order_func())
            count_query = select(func.count(DataSource.data_source_id)).where(and_(*filters))

            data_sources = list((await self.session.exec(
                base_query.offset((page - 1) * per_page).limit(per_page)
            )).all())

            total_count = (await self.session.exec(count_query)).one()
            return data_sources, total_count
        except Exception:
            logger.exception(f"Error fetching paginated data sources for user {user_id}")
            raise

    async def get_all_sources(
        self,
        page: int = 1,
        per_page: int = DEFAULT_PAGE_SIZE,
        search: Optional[str] = None,
        data_source_type: Optional[DataSourceType] = None,
        user_id: Optional[int] = None,
        sort_by: str = "data_source_created_at",
        sort_order: str = "desc",
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> Tuple[List[DataSource], int]:
        """Admin: Retrieve all active data sources with optional filters."""
        try:
            page, per_page = self._validate_pagination(page, per_page)

            filters = [DataSource.data_source_is_active.is_(True)]

            if search:
                search_term = f"%{search.lower()}%"
                filters.append(or_(
                    func.lower(DataSource.data_source_name).like(search_term),
                    func.lower(DataSource.data_source_url).like(search_term)
                ))
            if data_source_type:
                filters.append(DataSource.data_source_type == data_source_type)
            if user_id:
                filters.append(DataSource.data_source_user_id == user_id)
            if date_from:
                filters.append(DataSource.data_source_created_at >= date_from)
            if date_to:
                filters.append(DataSource.data_source_created_at <= date_to)

            sort_column = getattr(DataSource, sort_by, DataSource.data_source_created_at)
            sort_order_func = sort_column.desc if sort_order.lower() == "desc" else sort_column.asc

            base_query = select(DataSource).where(and_(*filters)).order_by(sort_order_func())
            count_query = select(func.count(DataSource.data_source_id)).where(and_(*filters))

            data_sources = list((await self.session.exec(
                base_query.offset((page - 1) * per_page).limit(per_page)
            )).all())

            total_count = (await self.session.exec(count_query)).one()
            return data_sources, total_count
        except Exception:
            logger.exception("Error fetching admin data sources list")
            raise

    @staticmethod
    def _validate_pagination(page: int, per_page: int) -> Tuple[int, int]:
        """Ensure pagination params are within bounds."""
        page = max(1, page)
        per_page = max(1, min(per_page, MAX_PAGE_SIZE))
        return page, per_page
