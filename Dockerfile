# Use the official Python slim image as the base
FROM python:3.10-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire "app" directory into the container
COPY ./app ./app

# Expose the port FastAPI will run on
EXPOSE 8000

# Set the entrypoint and command to run Uvicorn
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]


