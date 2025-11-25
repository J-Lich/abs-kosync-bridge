
# Using Python 3.11 Slim (Debian) instead of Alpine
# Alpine + PyTorch/NumPy (required for Whisper) is extremely unstable on ARM
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies
# ffmpeg is strictly required for audio processing
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python deps
# Note: On ARM, this might take time to compile some wheels if pre-built ones aren't available
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Create volume directories
RUN mkdir -p /books /data

# Default command runs the daemon
CMD ["python", "src/main.py"]
