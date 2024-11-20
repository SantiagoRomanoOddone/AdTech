# Usa la imagen oficial de Python slim como base
FROM python:3.9-slim-bullseye

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar requirements.txt al contenedor
COPY requirements.txt .

# Instalar dependencias del sistema necesarias para construir paquetes de Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libffi-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el directorio "app" al contenedor
COPY ./app ./app

# Copy the .env file into the container
COPY .env /app/.env

# Exponer el puerto en el que FastAPI se ejecutará
EXPOSE 8000

# Establecer el punto de entrada y el comando para ejecutar Uvicorn
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
