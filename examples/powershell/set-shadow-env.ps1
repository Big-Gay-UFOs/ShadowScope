# If script execution is blocked in PowerShell, run this helper with:
# powershell -NoProfile -ExecutionPolicy Bypass -File .\\examples\\powershell\\set-shadow-env.ps1

param(
    [switch]$ForcePrompt
)

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$envFile = Join-Path $repoRoot ".env"

function Set-SessionEnvFromDotEnv {
    param([string]$Path)

    if (-not (Test-Path $Path)) { return }

    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        if ($line.StartsWith("#")) { return }

        $idx = $line.IndexOf("=")
        if ($idx -lt 1) { return }

        $name = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")

        if ($name -notin @(
            "DATABASE_URL",
            "SAM_API_KEY",
            "SAM_API_BASE_URL",
            "SAM_API_MAX_RETRIES",
            "SAM_API_BACKOFF_BASE",
            "SAM_API_TIMEOUT_SECONDS"
        )) {
            return
        }

        $existing = (Get-Item "Env:$name" -ErrorAction SilentlyContinue).Value
        if ([string]::IsNullOrWhiteSpace($existing)) {
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

Set-SessionEnvFromDotEnv -Path $envFile

if ($ForcePrompt -or [string]::IsNullOrWhiteSpace($env:SAM_API_KEY)) {
    $sec = Read-Host -Prompt "Paste SAM.gov Public API Key (input hidden)" -AsSecureString
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
    try {
        $env:SAM_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
    }
}

if ([string]::IsNullOrWhiteSpace($env:SAM_API_KEY)) {
    throw "SAM_API_KEY is still empty. Add it to .env or paste it when prompted."
}

if ($env:SAM_API_KEY -match "\s") {
    throw "SAM_API_KEY contains whitespace/newlines. Use the single key token only."
}

$len = $env:SAM_API_KEY.Length
$preview = if ($len -ge 12) {
    "{0}...{1}" -f $env:SAM_API_KEY.Substring(0, 6), $env:SAM_API_KEY.Substring($len - 6, 6)
} else {
    "<too short to preview>"
}

Write-Host ("SAM_API_KEY available for this session (len={0}, preview={1})" -f $len, $preview) -ForegroundColor Green

if (-not [string]::IsNullOrWhiteSpace($env:DATABASE_URL)) {
    Write-Host "DATABASE_URL is loaded for this session." -ForegroundColor Green
}

Write-Host "Tip: re-run this script in each new PowerShell window, or keep SAM_API_KEY in your local .env (never commit it)." -ForegroundColor Yellow

