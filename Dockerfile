# =============================================================================
# N8N-FounderStories Production Dockerfile
# =============================================================================
# Multi-stage build for optimized production image
# =============================================================================

# =============================================================================
# Stage 1: Builder - Install dependencies and download models
# =============================================================================
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Download spacy model
RUN python -m spacy download en_core_web_sm

# =============================================================================
# Stage 2: Runtime - Create minimal production image
# =============================================================================
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app /app/credentials /app/logs && \
    chown -R appuser:appuser /app

# Install runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Required for Playwright
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    # Required for PDF processing
    tesseract-ocr \
    poppler-utils \
    # Cleanup
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser pyproject.toml .
COPY --chown=appuser:appuser .env.example .

# Install Playwright browsers as appuser
USER appuser
RUN playwright install chromium

# Switch back to root for final setup
USER root

# Create volume mount points
VOLUME ["/app/credentials", "/app/logs", "/app/crawl4ai-profile"]

# Expose application port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/api/v1/health', timeout=5)"

# Switch to non-root user
USER appuser

# Set entrypoint
ENTRYPOINT ["python", "-m", "n8n_founderstories"]

# =============================================================================
# Build Instructions:
# =============================================================================
# docker build -t n8n-founderstories:latest .
#
# Run Instructions:
# =============================================================================
# docker run -d \
#   --name n8n-founderstories \
#   -p 8000:8000 \
#   --env-file .env \
#   -v $(pwd)/credentials:/app/credentials:ro \
#   -v n8n-logs:/app/logs \
#   -v n8n-crawl-profile:/app/crawl4ai-profile \
#   --restart unless-stopped \
#   n8n-founderstories:latest
# =============================================================================