<#
.SYNOPSIS
  End-to-end smoke test of every required endpoint (Windows PowerShell 5.1+ / PowerShell 7).
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1 -BaseUrl http://localhost:8000
#>
param([string]$BaseUrl = "http://localhost:8000")

$ErrorActionPreference = "Stop"
$stamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$tenant = "smoke-$stamp"
$other = "smoke-other-$stamp"

function Invoke-Api {
    param([string]$Method, [string]$Path, [hashtable]$Headers = @{}, $Body = $null)
    $request = @{ Method = $Method; Uri = "$BaseUrl$Path"; Headers = $Headers; UseBasicParsing = $true }
    if ($null -ne $Body) {
        $request.Body = ($Body | ConvertTo-Json -Depth 5)
        $request.ContentType = "application/json"
    }
    try {
        $response = Invoke-WebRequest @request
        $json = $null
        if ($response.Content) { $json = $response.Content | ConvertFrom-Json }
        return @{ Status = [int]$response.StatusCode; Json = $json }
    }
    catch {
        $failed = $_.Exception.Response
        if ($null -eq $failed) { throw }
        return @{ Status = [int]$failed.StatusCode; Json = $null }
    }
}

function Assert-Status([string]$Name, [int[]]$Expected, [int]$Actual) {
    if ($Expected -contains $Actual) { Write-Host "  ok    $Name -> $Actual" }
    else { throw "FAIL $Name : expected $($Expected -join '/'), got $Actual" }
}

Write-Host "Smoke testing $BaseUrl (tenant $tenant)"
$h = @{ "X-Tenant-ID" = $tenant }

Assert-Status "GET /health" @(200) (Invoke-Api GET "/health").Status

$created = Invoke-Api POST "/documents" $h @{
    title = "Smoke test document"; content = "Distributed search smoke test."; tags = @("smoke"); metadata = @{ suite = "ci" }
}
Assert-Status "POST /documents" @(201, 202) $created.Status
$docId = $created.Json.id

$found = $false
for ($i = 0; $i -lt 20 -and -not $found; $i++) {
    $search = Invoke-Api GET "/search?q=smoke&tenant=$tenant"
    if ($search.Json.total -eq 1) { $found = $true } else { Start-Sleep -Milliseconds 500 }
}
if (-not $found) { throw "FAIL document never became searchable" }
Write-Host "  ok    GET /search?q=smoke&tenant=... -> 1 result"

Assert-Status "GET /documents/{id}" @(200) (Invoke-Api GET "/documents/$docId" $h).Status
Assert-Status "GET /documents/{id} as other tenant" @(404) (Invoke-Api GET "/documents/$docId" @{ "X-Tenant-ID" = $other }).Status
Assert-Status "GET /search without tenant" @(400) (Invoke-Api GET "/search?q=smoke").Status
Assert-Status "DELETE /documents/{id}" @(204) (Invoke-Api DELETE "/documents/$docId" $h).Status
Assert-Status "GET /documents/{id} after delete" @(404) (Invoke-Api GET "/documents/$docId" $h).Status

Write-Host "All smoke checks passed."
