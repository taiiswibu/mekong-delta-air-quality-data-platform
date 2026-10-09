$ErrorActionPreference = "Stop"
docker compose up -d --build
Write-Host "Airflow:   http://localhost:8080"
Write-Host "Kafka UI:  http://localhost:8081"
Write-Host "Streamlit: http://localhost:8501"
