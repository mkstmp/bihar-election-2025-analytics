# Use official lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Cloud Run provides $PORT, but we set a default to 8080 just in case
ENV PORT=8080

# Install system dependencies
# (Kept gcc just in case, though often not needed for modern wheels)
RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the port (Informational only)
EXPOSE 8080

# Command to run the app using Gunicorn
# CHANGE 1: Switched to "Shell Form" (no brackets) so we can use $PORT variable
# CHANGE 2: Bind to 0.0.0.0:$PORT instead of hardcoded 8000
CMD exec gunicorn main:app \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:$PORT \
    --timeout 120