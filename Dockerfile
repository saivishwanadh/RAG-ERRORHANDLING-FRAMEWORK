# ==============================================================================
# 1. Builder Stage: Compile dependencies
# ==============================================================================
FROM python:3.11-slim AS builder

WORKDIR /app

# Install system dependencies required for building Python packages (e.g., psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the spaCy language model during image build.
# This prevents the 400MB download on every container start/restart.
# The model is required by Presidio's LogSanitizer (maskdata.py).
RUN python -m spacy download en_core_web_lg

# Pre-warm the tldextract Public Suffix List cache during build.
# Without this, tldextract tries to write the cache to /home/appuser/.cache at
# runtime, which fails because appuser has no home directory → noisy WARNING logs.
# TLDEXTRACT_CACHE_DIR=/tmp is set in the runner stage so it always uses /tmp.
RUN TLDEXTRACT_CACHE_DIR=/tmp python -c \
    "import tldextract; e = tldextract.TLDExtract(cache_dir='/tmp'); e('example.com')"

# ==============================================================================
# 2. Runner Stage: Minimal runtime image
# ==============================================================================
FROM python:3.11-slim AS runner

WORKDIR /app
ENV PYTHONPATH=/app
# Tell tldextract to use /tmp (world-writable) for its cache.
# Prevents 'Permission denied: /home/appuser' warning since appuser has no home dir.
ENV TLDEXTRACT_CACHE_DIR=/tmp

# Install ONLY runtime libraries (e.g., libpq for Postgres)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Copy virtual environment from builder // COPY --from=builder /app/venv /app/venv
COPY --from=builder /app/venv /app/venv

# Activate virtual environment
ENV PATH="/app/venv/bin:$PATH"

# Copy application source code
COPY src/ ./src/
COPY UI/ ./UI/
COPY uvicorn_log_config.json ./uvicorn_log_config.json

# Chown directory to appuser (in case app needs to write logs locally, though stdout is better)
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Default command (will be overridden by docker-compose)
CMD ["python"]
