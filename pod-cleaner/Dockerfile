# Pod Cleaner Dockerfile
# Multi-stage build for smaller final image

# Build stage
FROM python:3.14-slim AS builder

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Production stage
FROM python:3.14-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

# Copy installed packages from builder to system path
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ ./src/

# Environment variables with defaults
ENV CLEANUP_INTERVAL_MINUTES=10 \
    HEALTH_PORT=8080 \
    MAX_WORKERS=10 \
    LOG_LEVEL=INFO \
    PYTHONUNBUFFERED=1

# Expose health check port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

# Switch to non-root user (numeric UID required for K8s runAsNonRoot validation)
USER 1000

# Run the application
CMD ["python", "-m", "src.main"]
