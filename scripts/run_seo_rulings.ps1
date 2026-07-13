# run_seo_rulings.ps1 - Trigger SEO Rulings checks via the dm-dashboard API
# Scheduled daily at 07:00 via Windows Task Scheduler

$ErrorActionPreference = "Continue"
# Prevent Invoke-WebRequest from hanging in non-interactive sessions (Task Scheduler)
# by disabling the progress bar that can't render without a console.
$ProgressPreference = "SilentlyContinue"
$logDir  = "C:\Users\l.davidowski\dm-dashboard\logs"
$logFile = Join-Path $logDir ("seo_rulings_{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))
$baseUrl = "https://win-htz-006.colo.beslist.net:3003"
$apiUrl  = "$baseUrl/api/seo-rulings/run"

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

# Trust all certs for internal server (Windows PowerShell 5.1 compatible)
# Uses a compiled C# callback — PowerShell scriptblock callbacks fail in
# Task Scheduler because there is no runspace on the callback thread.
$needsCompile = -not ([System.Management.Automation.PSTypeName]'TrustAllCerts').Type
if ($needsCompile) {
Add-Type @"
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
public class TrustAllCerts : ICertificatePolicy {
    public bool CheckValidationResult(ServicePoint sp, X509Certificate cert, WebRequest req, int problem) { return true; }
    public static bool Callback(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors errors) { return true; }
}
"@
}
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCerts
[System.Net.ServicePointManager]::ServerCertificateValidationCallback = [System.Net.Security.RemoteCertificateValidationCallback]::new([TrustAllCerts], 'Callback')
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

function Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $logFile -Value $line
    Write-Host $line
}

Log "=== SEO Rulings scheduled run start ==="
if ($needsCompile) { Log "TLS trust helper compiled" }
Log "TLS configured"

# Read password from .env file (auth is optional — server skips it when unset)
$envFile = "C:\Users\l.davidowski\dm-dashboard\.env"
$password = (Get-Content $envFile | Where-Object { $_ -match "^DASHBOARD_PASSWORD=" }) -replace "^DASHBOARD_PASSWORD=", ""

# Retry helper
function Invoke-WithRetry($Label, $MaxAttempts, $DelaySec, $ScriptBlock) {
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            return (& $ScriptBlock)
        } catch {
            Log "WARN: $Label attempt $i/$MaxAttempts failed: $_"
            if ($i -lt $MaxAttempts) {
                Log "       Retrying in ${DelaySec}s..."
                Start-Sleep -Seconds $DelaySec
            } else {
                throw $_
            }
        }
    }
}

# 1. Login and get session cookie (skip when auth is disabled)
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

if ($password) {
    Log "Password loaded"
    $loginUrl = "$baseUrl/login"
    $body = "password=$password"

    Log "Logging in..."
    $loginOk = $false
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            $null = Invoke-WebRequest -Uri $loginUrl -Method POST -Body $body `
                -ContentType "application/x-www-form-urlencoded" `
                -WebSession $session -MaximumRedirection 0 -TimeoutSec 30 `
                -UseBasicParsing -ErrorAction SilentlyContinue
            $loginOk = $true
            break
        } catch {
            # A 303 redirect is expected on successful login - check if we got the cookie
            if ($session.Cookies.Count -gt 0) {
                $loginOk = $true
                break
            }
            Log "WARN: Login attempt $attempt/3 failed: $_"
            if ($attempt -lt 3) {
                Log "       Retrying in 30s..."
                Start-Sleep -Seconds 30
            }
        }
    }
    if ($loginOk) {
        Log "Authenticated with dashboard"
    } else {
        Log "ERROR: Failed to authenticate after 3 attempts"
        exit 1
    }
} else {
    Log "Auth disabled (no DASHBOARD_PASSWORD set) - skipping login"
}

# 2. Trigger the SEO Rulings checks (can take ~30-60s)
try {
    Log "Triggering SEO Rulings checks..."
    $result = Invoke-WithRetry "API call" 2 15 {
        Invoke-RestMethod -Uri $apiUrl -Method POST -TimeoutSec 300 `
            -ContentType "application/json" -WebSession $session
    }

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
    Log "ERROR: Failed to run SEO Rulings checks after retries"
    Log "       $_"
    exit 1
}

Log "=== SEO Rulings scheduled run end ==="
