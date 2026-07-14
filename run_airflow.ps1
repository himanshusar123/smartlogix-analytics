# Airflow Orchestration and Setup Script for Windows (Local Development)

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "SmartLogix - Airflow Orchestration Setup" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# 1. Set local Airflow Home
$airflowHomeDir = Join-Path $PSScriptRoot "airflow_home"
if (-not (Test-Path $airflowHomeDir)) {
    New-Item -ItemType Directory -Path $airflowHomeDir | Out-Null
    Write-Host "[Airflow] Created local Airflow home: $airflowHomeDir" -ForegroundColor Green
}

# Export environment variables for the session
$env:AIRFLOW_HOME = $airflowHomeDir
$env:AIRFLOW__CORE__DAGS_FOLDER = Join-Path $PSScriptRoot "dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
Write-Host "[Airflow] AIRFLOW_HOME set to: $env:AIRFLOW_HOME" -ForegroundColor Green
Write-Host "[Airflow] DAGS_FOLDER configured to: $env:AIRFLOW__CORE__DAGS_FOLDER" -ForegroundColor Green

# 2. Check if Airflow is installed
if (-not (Get-Command airflow -ErrorAction SilentlyContinue)) {
    Write-Host "[Airflow] Apache Airflow not found in python path. Installing apache-airflow..." -ForegroundColor Yellow
    python -m pip install "apache-airflow==2.9.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.10.txt"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[Airflow] Constraint installation failed. Attempting basic install..." -ForegroundColor Yellow
        python -m pip install apache-airflow
    }
} else {
    Write-Host "[Airflow] Apache Airflow is already installed." -ForegroundColor Green
}

# 3. Initialize Airflow Database
Write-Host "[Airflow] Initializing database (airflow db migrate)..." -ForegroundColor Yellow
airflow db migrate

# 4. Create Admin User if not exists
Write-Host "[Airflow] Creating admin user (airflow users create)..." -ForegroundColor Yellow
# Ignoring error if user already exists
airflow users create `
    --username admin `
    --firstname Smart `
    --lastname Logix `
    --role Admin `
    --email admin@smartlogix.com `
    --password admin 2>$null

# 5. Start Airflow Components in separate processes
Write-Host "[Airflow] Starting Scheduler in a new terminal..." -ForegroundColor Cyan
$schedulerCmd = "`$env:AIRFLOW_HOME='$airflowHomeDir'; `$env:AIRFLOW__CORE__DAGS_FOLDER='$($env:AIRFLOW__CORE__DAGS_FOLDER)'; airflow scheduler"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$PSScriptRoot'; Write-Host '--- Airflow Scheduler ---' -ForegroundColor Yellow; $schedulerCmd"

Write-Host "[Airflow] Starting Webserver in a new terminal..." -ForegroundColor Cyan
$webserverCmd = "`$env:AIRFLOW_HOME='$airflowHomeDir'; `$env:AIRFLOW__CORE__DAGS_FOLDER='$($env:AIRFLOW__CORE__DAGS_FOLDER)'; airflow webserver --port 8080"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$PSScriptRoot'; Write-Host '--- Airflow Webserver ---' -ForegroundColor Cyan; $webserverCmd"

Write-Host "=============================================" -ForegroundColor Green
Write-Host "Airflow setup complete and components launched!" -ForegroundColor Green
Write-Host "Please wait a few seconds and then access the UI at:" -ForegroundColor Green
Write-Host "  URL:      http://localhost:8080" -ForegroundColor Cyan
Write-Host "  Username: admin" -ForegroundColor Cyan
Write-Host "  Password: admin" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Green
