param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not $env:JAVA_HOME) {
    Write-Host "请先设置 JAVA_HOME（Java 17+）" -ForegroundColor Yellow
}
if ($env:HADOOP_HOME) {
    $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
}

& $Python -m src.etl.spark_job --input data/raw --warehouse data/warehouse --output data/warehouse_spark
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $Python -m src.dq.spark_runner --raw data/raw --warehouse data/warehouse --output reports/dq_spark.json
exit $LASTEXITCODE
