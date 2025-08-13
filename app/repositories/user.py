from typing import Optional, List, Dict, Any, Tuple
from sqlmodel import select, func, or_, and_
from datetime import datetime
from app.models import User, UserProfile
from app.config.database import SessionDep


class UserRepository:
    """Repository class for handling User database operations."""
    
    def __init__(self, db_session: SessionDep): # type: ignore
        self.session: SessionDep = db_session # type: ignore
    
    async def get_user_by_id(self, user_id: int, include_profile: bool = False) -> Optional[User]:
        """
        Get user by ID.
        
        Args:
            user_id: The user ID to search for
            include_profile: Whether to include user profile information
            
        Returns:
            User object if found, None otherwise
        """
        if include_profile:
            statement = (
                select(User, UserProfile)
                .outerjoin(UserProfile, User.user_id == UserProfile.user_profile_user_id)
                .where(User.user_id == user_id)
            )
            result = await self.session.exec(statement)
            user_data = result.first()
            
            if user_data:
                user, profile = user_data
                if profile:
                    user.user_profile = profile
                return user
            return None
        else:
            return await self.session.get(User, user_id)
    
    async def get_user_by_email(self, email: str, include_profile: bool = False) -> Optional[User]:
        """
        Get user by email address.
        
        Args:
            email: The email address to search for
            include_profile: Whether to include user profile information
            
        Returns:
            User object if found, None otherwise
        """
        if include_profile:
            statement = (
                select(User, UserProfile)
                .outerjoin(UserProfile, User.user_id == UserProfile.user_profile_user_id)
                .where(User.user_email == email)
            )
            result = await self.session.exec(statement)
            user_data = result.first()
            
            if user_data:
                user, profile = user_data
                if profile:
                    user.user_profile = profile
                return user
            return None
        else:
            statement = select(User).where(User.user_email == email)
            result = await self.session.exec(statement)
            return result.first()
    
    async def get_users_list(
        self,
        page: int = 1,
        per_page: int = 10,
        search: Optional[str] = None,
        is_active: Optional[bool] = None,
        is_verified: Optional[bool] = None,
        sort_by: str = "created_at",
        sort_order: str = "desc",
        include_profiles: bool = False,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> Tuple[List[User], int]:
        """
        Get paginated list of users with filtering and search.
        
        Args:
            page: Page number (starting from 1)
            per_page: Number of users per page
            search: Search term to filter by name or email
            is_active: Filter by active status
            is_verified: Filter by verification status
            sort_by: Field to sort by (created_at, updated_at, user_email, user_first_name, user_last_name)
            sort_order: Sort order (asc, desc)
            include_profiles: Whether to include user profiles
            date_from: Filter users created from this date
            date_to: Filter users created before this date
            
        Returns:
            Tuple of (users_list, total_count)
        """
        # Validate pagination parameters
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 10
        if per_page > 100:  # Limit max per_page to prevent performance issues
            per_page = 100
        
        # Calculate offset
        offset = (page - 1) * per_page
        
        # Build base query
        if include_profiles:
            statement = (
                select(User, UserProfile)
                .outerjoin(UserProfile, User.user_id == UserProfile.user_profile_user_id)
            )
            count_statement = select(func.count(User.user_id))
        else:
            statement = select(User)
            count_statement = select(func.count(User.user_id))
        
        # Apply filters
        filters = []
        
        # Search filter
        if search:
            search_term = f"%{search.lower()}%"
            search_filters = [
                func.lower(User.user_first_name).like(search_term),
                func.lower(User.user_last_name).like(search_term),
                func.lower(User.user_email).like(search_term),
                func.lower(func.concat(User.user_first_name, " ", User.user_last_name)).like(search_term)
            ]
            filters.append(or_(*search_filters))
        
        # Active status filter
        if is_active is not None:
            filters.append(User.user_is_active == is_active)
        
        # Verified status filter
        if is_verified is not None:
            filters.append(User.user_is_verified == is_verified)
        
        # Date range filters
        if date_from:
            filters.append(User.user_created_at >= date_from)
        
        if date_to:
            filters.append(User.user_created_at <= date_to)
        
        # Apply all filters
        if filters:
            statement = statement.where(and_(*filters))
            count_statement = count_statement.where(and_(*filters))
        
        # Apply sorting
        sort_field = getattr(User, sort_by, None)
        if sort_field is None:
            sort_field = User.user_created_at  # Default sort field
        
        if sort_order.lower() == "asc":
            statement = statement.order_by(sort_field.asc())
        else:
            statement = statement.order_by(sort_field.desc())
        
        # Apply pagination
        statement = statement.offset(offset).limit(per_page)
        
        # Execute queries
        users_result = await self.session.exec(statement)
        count_result = await self.session.exec(count_statement)
        
        total_count = count_result.one()
        
        if include_profiles:
            users = []
            for user_data in users_result:
                if isinstance(user_data, tuple):
                    user, profile = user_data
                    if profile:
                        user.user_profile = profile
                    users.append(user)
                else:
                    users.append(user_data)
        else:
            users = list(users_result)
        
        return users, total_count
    
    async def search_users(
        self,
        search_term: str,
        limit: int = 20,
        include_profiles: bool = False
    ) -> List[User]:
        """
        Search users by name or email.
        
        Args:
            search_term: Term to search for
            limit: Maximum number of results
            include_profiles: Whether to include user profiles
            
        Returns:
            List of users matching the search term
        """
        search_pattern = f"%{search_term.lower()}%"
        
        if include_profiles:
            statement = (
                select(User, UserProfile)
                .outerjoin(UserProfile, User.user_id == UserProfile.user_profile_user_id)
                .where(
                    or_(
                        func.lower(User.user_first_name).like(search_pattern),
                        func.lower(User.user_last_name).like(search_pattern),
                        func.lower(User.user_email).like(search_pattern),
                        func.lower(func.concat(User.user_first_name, " ", User.user_last_name)).like(search_pattern)
                    )
                )
                .limit(limit)
            )
            
            result = await self.session.exec(statement)
            users = []
            for user_data in result:
                if isinstance(user_data, tuple):
                    user, profile = user_data
                    if profile:
                        user.user_profile = profile
                    users.append(user)
                else:
                    users.append(user_data)
            return users
        else:
            statement = (
                select(User)
                .where(
                    or_(
                        func.lower(User.user_first_name).like(search_pattern),
                        func.lower(User.user_last_name).like(search_pattern),
                        func.lower(User.user_email).like(search_pattern),
                        func.lower(func.concat(User.user_first_name, " ", User.user_last_name)).like(search_pattern)
                    )
                )
                .limit(limit)
            )
            
            result = await self.session.exec(statement)
            return list(result)

