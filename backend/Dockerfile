FROM python:3.11-slim

# WORKDIR /app — backend source goes into /app/backend/
# so `from ..db.session import ...` resolves correctly as relative imports
# within the `backend` package.
WORKDIR /app

# Install dependencies first (better Docker layer caching)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy backend source into /app/backend/ (not /app/ directly)
COPY . ./backend/

EXPOSE 8000
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]