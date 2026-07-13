# SmartLogix Pipeline Orchestration Script

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "SmartLogix Logistics - Pipeline Orchestration" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# 1. Check and configure Java
$localJdkDir = Join-Path $PSScriptRoot "jdk"
$jdkZip = Join-Path $PSScriptRoot "jdk.zip"

if (-not $env:JAVA_HOME -and -not (Get-Command java -ErrorAction SilentlyContinue)) {
    if (-not (Test-Path $localJdkDir)) {
        Write-Host "[JDK] Java not found. Downloading portable Eclipse Temurin JDK 11..." -ForegroundColor Yellow
        $url = "https://api.adoptium.net/v3/binary/latest/11/ga/windows/x64/jdk/hotspot/normal/adoptium"
        
        # Download ZIP
        Invoke-WebRequest -Uri $url -OutFile $jdkZip -UserAgent "Mozilla/5.0"
        
        Write-Host "[JDK] Extracting JDK 11..." -ForegroundColor Yellow
        New-Item -ItemType Directory -Force -Path $localJdkDir | Out-Null
        Expand-Archive -Path $jdkZip -DestinationPath $localJdkDir -Force
        
        # Cleanup zip
        Remove-Item $jdkZip -Force
        Write-Host "[JDK] Portable JDK 11 installed successfully." -ForegroundColor Green
    }
    
    # Configure JAVA_HOME dynamically
    $extractedFolder = Get-ChildItem $localJdkDir | Where-Object { $_.PSIsContainer } | Select-Object -First 1
    $env:JAVA_HOME = $extractedFolder.FullName
    $env:PATH = "$(Join-Path $env:JAVA_HOME 'bin');$env:PATH"
    Write-Host "[JDK] JAVA_HOME set to: $env:JAVA_HOME" -ForegroundColor Green
} else {
    Write-Host "[JDK] Java environment already detected." -ForegroundColor Green
}

# 1b. Check and configure Hadoop winutils for Spark
$hadoopDir = Join-Path $PSScriptRoot "hadoop"
$hadoopBinDir = Join-Path $hadoopDir "bin"
$winutilsFile = Join-Path $hadoopBinDir "winutils.exe"
$hadoopDllFile = Join-Path $hadoopBinDir "hadoop.dll"

if (-not $env:HADOOP_HOME -and -not (Test-Path $winutilsFile)) {
    Write-Host "[Hadoop] HADOOP_HOME not set. Downloading portable winutils.exe and hadoop.dll..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Force -Path $hadoopBinDir | Out-Null
    
    $winutilsUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe"
    $hadoopDllUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll"
    
    Invoke-WebRequest -Uri $winutilsUrl -OutFile $winutilsFile -UserAgent "Mozilla/5.0"
    Invoke-WebRequest -Uri $hadoopDllUrl -OutFile $hadoopDllFile -UserAgent "Mozilla/5.0"
    
    Write-Host "[Hadoop] Portable winutils and hadoop.dll installed." -ForegroundColor Green
}

# Set HADOOP_HOME environment variable for this session
if (-not $env:HADOOP_HOME) {
    $env:HADOOP_HOME = $hadoopDir
    $env:PATH = "$hadoopBinDir;$env:PATH"
    Write-Host "[Hadoop] HADOOP_HOME set to: $env:HADOOP_HOME" -ForegroundColor Green
} else {
    Write-Host "[Hadoop] HADOOP_HOME already configured." -ForegroundColor Green
}

# 2. Check and start Docker containers
Write-Host "[Docker] Starting Zookeeper and Kafka..." -ForegroundColor Yellow
docker compose down -v 2>$null | Out-Null
docker compose up -d

Write-Host "[Docker] Waiting for Kafka to be fully ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# 3. Install Python Dependencies
Write-Host "[Pip] Installing Python requirements..." -ForegroundColor Yellow
python -m pip install -r requirements.txt

# 4. Start Pipeline components in separate terminals for visual monitoring
Write-Host "[Launch] Opening components in separate consoles..." -ForegroundColor Green

# Prepare dynamic environment settings to pass to child processes
$envCommand = ""
if ($env:JAVA_HOME) {
    $envCommand += "`$env:JAVA_HOME='$($env:JAVA_HOME.Replace('\','\\'))'; `$env:PATH='$($env:JAVA_HOME.Replace('\','\\'))\bin;'+`$env:PATH;"
}
if ($env:HADOOP_HOME) {
    $envCommand += "`$env:HADOOP_HOME='$($env:HADOOP_HOME.Replace('\','\\'))'; `$env:PATH='$($env:HADOOP_HOME.Replace('\','\\'))\bin;'+`$env:PATH;"
}

# Terminal 2: Producer
Write-Host "[Launch] Starting Producer in a new terminal..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$PSScriptRoot'; Write-Host '--- Kafka Producer ---' -ForegroundColor Green; python producer.py"

# Terminal 3: Spark Streaming
Write-Host "[Launch] Starting Spark Streaming in a new terminal..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$PSScriptRoot'; $envCommand Write-Host '--- PySpark Structured Streaming ---' -ForegroundColor Yellow; python spark_streaming.py"

# Wait a few seconds for Spark to start before booting Streamlit
Start-Sleep -Seconds 5

# Terminal 4: Streamlit Dashboard
Write-Host "[Launch] Starting Streamlit Dashboard in a new terminal..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$PSScriptRoot'; Write-Host '--- Streamlit Dashboard ---' -ForegroundColor Cyan; streamlit run dashboard.py"

Write-Host "=============================================" -ForegroundColor Green
Write-Host "SmartLogix Pipeline Successfully Launched!" -ForegroundColor Green
Write-Host "Consoles opened:" -ForegroundColor Green
Write-Host "  - Terminal 1 (this): Docker containers running" -ForegroundColor Gray
Write-Host "  - Terminal 2: Shipment Event Producer" -ForegroundColor Gray
Write-Host "  - Terminal 3: PySpark Streaming Processor" -ForegroundColor Gray
Write-Host "  - Terminal 4: Streamlit Analytics Dashboard" -ForegroundColor Gray
Write-Host "=============================================" -ForegroundColor Green
