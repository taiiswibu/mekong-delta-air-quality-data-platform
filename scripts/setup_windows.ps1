$ErrorActionPreference = "Stop"

Write-Host "[1/6] Checking Docker..."
docker version | Out-Null

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

Write-Host "[2/6] Creating virtual environment..."
if (-not (Test-Path ".venv")) {
    py -3.12 -m venv .venv
}

Write-Host "[3/6] Installing local tools..."
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
& .\.venv\Scripts\python.exe -m pip install -e . --no-deps

Write-Host "[4/6] Generating Snowflake RSA key pair..."
if (-not (Test-Path ".secrets\rsa_key.p8")) {
    & .\.venv\Scripts\python.exe scripts\generate_keypair.py
} else {
    Write-Host "RSA key already exists; keeping it."
}

Write-Host "[5/6] Generating deterministic sample data..."
& .\.venv\Scripts\python.exe scripts\generate_seed.py

Write-Host "[6/6] Next steps"
Write-Host "1. Edit .env: Snowflake account/user and WAQI token."
Write-Host "2. Run the ALTER USER RSA_PUBLIC_KEY command printed by generate_keypair.py in Snowsight."
Write-Host "3. Run: .\.venv\Scripts\python.exe scripts\bootstrap_snowflake.py"
Write-Host "4. Run: docker compose up -d --build"
