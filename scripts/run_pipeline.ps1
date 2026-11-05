param(
    [switch]$WithTests = $true
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

python -m src.pipeline
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($WithTests) {
    python -m pytest -q
    exit $LASTEXITCODE
}
