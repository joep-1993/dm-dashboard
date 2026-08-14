FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
# backend/vendor/ is copied first so local tarballs referenced from requirements.txt
# (e.g. the Google Search Ads 360 SDK) resolve during pip install.
COPY requirements.txt .
COPY backend/vendor/ /app/backend/vendor/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY backend/ /app/backend/
COPY frontend/ /app/frontend/

# Expose port
EXPOSE 8000

# Run with auto-reload for development
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
