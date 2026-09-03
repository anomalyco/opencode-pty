# Manual-only diagnostic delivery. No payload is stored in Git or uploaded.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$payload = $env:OPENCODE_PTY_PROBE_GZIP_BASE64
$expected = $env:OPENCODE_PTY_PROBE_SHA256
if ([string]::IsNullOrEmpty($payload) -or $payload.Length -gt 24000) {
    throw 'Probe payload must contain 1..24000 characters'
}
if ($payload -cnotmatch '\A[A-Za-z0-9+/]+={0,2}\z' -or $payload.Length % 4 -ne 0) {
    throw 'Probe payload must be single-line base64'
}
if ($expected -cnotmatch '\A[0-9a-fA-F]{64}\z') {
    throw 'Probe requires a SHA-256 digest'
}

$path = Join-Path (Split-Path -Parent $PSScriptRoot) 'tests/windows-probe.rs'
if (Test-Path -LiteralPath $path) {
    throw 'Reserved ephemeral probe path already exists'
}

$compressed = [Convert]::FromBase64String($payload)
$inputStream = [IO.MemoryStream]::new($compressed)
$gzip = [IO.Compression.GZipStream]::new($inputStream, [IO.Compression.CompressionMode]::Decompress)
$decoded = [IO.MemoryStream]::new()
try {
    $buffer = [byte[]]::new(8192)
    while (($count = $gzip.Read($buffer, 0, $buffer.Length)) -gt 0) {
        if ($decoded.Length + $count -gt 262144) {
            throw 'Decompressed probe exceeds 256 KiB'
        }
        $decoded.Write($buffer, 0, $count)
    }
    $source = $decoded.ToArray()
}
finally {
    $decoded.Dispose()
    $gzip.Dispose()
    $inputStream.Dispose()
}

# Validate strict UTF-8 without re-encoding: the verified bytes are written as-is.
[void] [Text.UTF8Encoding]::new($false, $true).GetString($source)
$digest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($source)).ToLowerInvariant()
if ($digest -cne $expected.ToLowerInvariant()) {
    throw 'Probe source SHA-256 mismatch'
}

$created = $false
try {
    # CreateNew also protects against an existing entry appearing after Test-Path.
    $file = [IO.File]::Open($path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    $created = $true
    try {
        $file.Write($source, 0, $source.Length)
    }
    finally {
        $file.Dispose()
    }
    $env:OPENCODE_PTY_PROBE_GZIP_BASE64 = $null
    $env:OPENCODE_PTY_PROBE_SHA256 = $null
    Write-Output "Ephemeral probe SHA256=$digest target=$env:CARGO_BUILD_TARGET"
    # Short compiler diagnostics avoid dumping source snippets into the job log.
    & cargo test --locked --all-features --test windows-probe --message-format=short -- --test-threads=1 --nocapture
    if ($LASTEXITCODE -ne 0) {
        throw "Ephemeral probe failed with exit code $LASTEXITCODE"
    }
}
finally {
    if ($created) {
        Remove-Item -LiteralPath $path -Force
    }
}
