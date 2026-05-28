# run_seo_rulings.ps1 - Trigger SEO Rulings checks via the dm-dashboard API
# Scheduled daily at 07:00 via Windows Task Scheduler

$ErrorActionPreference = "Continue"
$logDir  = "C:\Users\l.davidowski\dm-dashboard\logs"
$logFile = Join-Path $logDir ("seo_rulings_{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))
$baseUrl = "https://win-htz-006.colo.beslist.net:3003"
$apiUrl  = "$baseUrl/api/seo-rulings/run"

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

# Trust all certs for internal server (Windows PowerShell 5.1 compatible)
Add-Type @"
using System.Net;
using System.Security.Cryptography.X509Certificates;
public class TrustAll : ICertificatePolicy {
    public bool CheckValidationResult(ServicePoint sp, X509Certificate cert, WebRequest req, int problem) { return true; }
}
"@
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAll
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

function Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $logFile -Value $line
    Write-Host $line
}

Log "=== SEO Rulings scheduled run start ==="

# Read password from .env file
$envFile = "C:\Users\l.davidowski\dm-dashboard\.env"
$password = (Get-Content $envFile | Where-Object { $_ -match "^DASHBOARD_PASSWORD=" }) -replace "^DASHBOARD_PASSWORD=", ""
if (-not $password) {
    Log "ERROR: DASHBOARD_PASSWORD not found in .env"
    exit 1
}

# 1. Login and get session cookie
try {
    $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
    $loginUrl = "$baseUrl/login"
    $body = "password=$password"
    $null = Invoke-WebRequest -Uri $loginUrl -Method POST -Body $body -ContentType "application/x-www-form-urlencoded" -WebSession $session -MaximumRedirection 0 -ErrorAction SilentlyContinue
    Log "Authenticated with dashboard"
} catch {
    # A 303 redirect is expected on successful login - check if we got the cookie
    if ($session.Cookies.Count -gt 0) {
        Log "Authenticated with dashboard"
    } else {
        Log "ERROR: Failed to authenticate with dashboard"
        Log "       $_"
        exit 1
    }
}

# 2. Trigger the SEO Rulings checks (can take ~30-60s)
try {
    Log "Triggering SEO Rulings checks..."
    $result = Invoke-RestMethod -Uri $apiUrl -Method POST -TimeoutSec 300 -ContentType "application/json" -WebSession $session

    $passed = $result.summary.passed
    $failed = $result.summary.failed
    $nPassed = @($passed).Count
    $nFailed = @($failed).Count

    Log "Checks complete: $nPassed passed, $nFailed failed"

    if ($nPassed -gt 0) {
        foreach ($name in $passed) { Log "  PASS: $name" }
    }
    if ($nFailed -gt 0) {
        Log "WARNING: $nFailed check(s) failed!"
        foreach ($name in $failed) { Log "  FAIL: $name" }
    }

    Log "Slack: $($result.summary.slack)"
} catch {
    Log "ERROR: Failed to run SEO Rulings checks"
    Log "       $_"
    exit 1
}

Log "=== SEO Rulings scheduled run end ==="
