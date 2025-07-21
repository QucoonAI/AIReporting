import ssl
from typing import Annotated, AsyncGenerator, Optional
from fastapi import Depends
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError
from .settings import get_settings


settings = get_settings()
SessionDep: Annotated[Optional[AsyncSession], Depends] = Depends(lambda: None)

url = make_url(settings.DATABASE_URL)
ssl_context = ssl.create_default_context()

if url.query.get("sslmode") == "require":
    connect_args = {"ssl": ssl_context}
    query = dict(url.query)
    query.pop("sslmode", None)
    query.pop("channel_binding", None)
    url = url._replace(query=query)
else:
    connect_args = {}

async_engine = create_async_engine(
    url.render_as_string(hide_password=False), connect_args=connect_args, echo=False
)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_engine) as session:
        try:
            yield session
        except SQLAlchemyError as exc:
            await session.rollback()
            raise exc

SessionDep = Annotated[AsyncSession, Depends(get_session)]
