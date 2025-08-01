FROM ghcr.io/astral-sh/uv:0.7.2 AS uv

# Use a standard Python base image for local development
FROM python:3.13-slim AS builder

# Enable bytecode compilation for better performance
ENV UV_COMPILE_BYTECODE=1

# Disable installer metadata for deterministic builds
ENV UV_NO_INSTALLER_METADATA=1

# Enable copy mode to support bind mount caching
ENV UV_LINK_MODE=copy

# Set working directory
WORKDIR /app

# Install dependencies using uv
RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt && \
    uv pip install -r requirements.txt --target /app/dependencies

# Final stage for running locally
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy dependencies from builder stage
COPY --from=builder /app/dependencies /app/dependencies

# Copy application code
COPY ./app /app/app

# Add dependencies to Python path
ENV PYTHONPATH="/app/dependencies"

# Expose port for local development (adjust as needed)
EXPOSE 8000

# Set the command to run your application locally
CMD ["python", "-m", "uvicorn", "app.main:app", "--host=0.0.0.0", "--port=8000"]