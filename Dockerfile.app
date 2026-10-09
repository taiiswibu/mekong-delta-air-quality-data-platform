FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/project/src:/opt/project

WORKDIR /opt/project
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt
COPY . .
