param(
    [string]$Bootstrap = "127.0.0.1:9092",
    [string]$Topic = "ods-order-events"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

python -m src.kafka.real_kafka --bootstrap $Bootstrap --topic $Topic
exit $LASTEXITCODE
