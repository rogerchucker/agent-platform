FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install runtime deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code.
COPY control_plane ./control_plane

EXPOSE 8080

# Single process; in-memory state lives here by design.
CMD ["uvicorn", "control_plane.main:app", "--host", "0.0.0.0", "--port", "8080"]
