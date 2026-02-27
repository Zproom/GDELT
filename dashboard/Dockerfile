# This is a Dockerfile for the streamlit dashboard application. It sets up a 
# Python environment, installs dependencies, and runs the app.


FROM python:3.12-slim

# Set working directory inside container.
WORKDIR /app
RUN mkdir /app/data

# Install dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and data
COPY app.py .
COPY constants.py .
COPY data/gdelt_project.gold.suri_20260226.csv data/

# Streamlit runs on port 8501 by default.
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app.py"]