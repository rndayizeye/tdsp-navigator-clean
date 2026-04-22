# ============================================================================
# STAGE 1: Builder
# ============================================================================
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libgdal-dev \
    libproj-dev \
    libgeos-dev \
    libxml2-dev \
    libxslt-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY src/requirements.txt /build/requirements.txt

# Install Python packages
RUN pip install --user \
    "kedro~=0.19.14" \
    "kedro-datasets[pandas,geopandas,json,parquet]~=3.0" \
    "pyarrow>=10.0.0" \
    "pyogrio>=0.7.2" \
    "geopandas>=0.14.0" \
    "sodapy>=2.2.0" \
    "marimo>=0.1.0" \
    -r requirements.txt

# ============================================================================
# STAGE 2: Runtime
# ============================================================================
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH=/home/kedro/.local/bin:$PATH

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal32 \
    libproj25 \
    libgeos-c1v5 \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create user
RUN useradd -m -d /home/kedro -s /bin/bash kedro

# Copy Python packages from builder
COPY --from=builder --chown=kedro:kedro /root/.local /home/kedro/.local

# Copy application code
COPY --chown=kedro:kedro . /app/

# Install project
RUN pip install --user --no-deps -e .

USER kedro

EXPOSE 8080 4141

CMD ["marimo", "edit", "--host", "0.0.0.0", "-p", "8080", "--no-token"]