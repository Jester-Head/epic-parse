# Back up the epic_parse Postgres database.
#
#   powershell -ExecutionPolicy Bypass -File scripts\backup.ps1
#
# - Connection settings and password come from the project's .env file.
# - Writes a compressed dump to $BackupDir, checks it can be read, and keeps the newest $Keep.
# - Copies the newest dump to $OffsiteDir (a OneDrive folder) so a copy exists off this PC.
# - Appends to backup.log in $BackupDir. Exits non-zero on failure.
#
# Restore with:
#   pg_restore --create --dbname=postgres <file>.dump          (recreates the database)
#   pg_restore --clean --if-exists --dbname=epic_parse <file>.dump   (overwrites existing)

param(
    [string]$BackupDir = "$env:USERPROFILE\backups\epic_parse",
    [string]$OffsiteDir = "$env:USERPROFILE\OneDrive\Backups\epic_parse",
    [int]$Keep = 4
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PgBin = (Get-ChildItem "C:\Program Files\PostgreSQL\*\bin\pg_dump.exe" | Sort-Object FullName -Descending | Select-Object -First 1).DirectoryName
New-Item -ItemType Directory -Force $BackupDir | Out-Null
$Log = Join-Path $BackupDir "backup.log"

function Write-Log($msg) {
    $line = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $Log -Value $line -Encoding utf8
    Write-Output $line
}

try {
    # Load PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE from .env (pg_dump reads them from the environment).
    foreach ($line in Get-Content (Join-Path $ProjectRoot ".env")) {
        if ($line -match '^\s*(PG[A-Z]+)\s*=\s*(.*)$') { Set-Item "env:$($Matches[1])" $Matches[2].Trim() }
    }
    $db = if ($env:PGDATABASE) { $env:PGDATABASE } else { "epic_parse" }

    $stamp = Get-Date -Format "yyyy-MM-dd_HHmm"
    $final = Join-Path $BackupDir "${db}_$stamp.dump"
    $partial = "$final.partial"
    Write-Log "Starting backup of '$db' -> $final"
    $sw = [Diagnostics.Stopwatch]::StartNew()

    & "$PgBin\pg_dump.exe" --format=custom --compress=zstd:3 --file=$partial $db
    if ($LASTEXITCODE -ne 0) { throw "pg_dump failed with exit code $LASTEXITCODE" }

    # A dump that pg_restore can't list is not a backup.
    $entries = & "$PgBin\pg_restore.exe" --list $partial
    if ($LASTEXITCODE -ne 0 -or -not ($entries -match "TABLE DATA public posts")) { throw "Backup file failed verification" }
    Move-Item $partial $final

    $sizeGB = (Get-Item $final).Length / 1GB
    Write-Log ("Backup OK: {0:N2} GB in {1:N0} min" -f $sizeGB, $sw.Elapsed.TotalMinutes)

    # Keep only the newest $Keep dumps locally.
    Get-ChildItem $BackupDir -Filter "${db}_*.dump" | Sort-Object Name -Descending | Select-Object -Skip $Keep |
        ForEach-Object { Remove-Item $_.FullName; Write-Log "Removed old backup $($_.Name)" }

    # One rolling copy off this PC.
    if ($OffsiteDir) {
        New-Item -ItemType Directory -Force $OffsiteDir | Out-Null
        $offsite = Join-Path $OffsiteDir "${db}_latest.dump"
        Copy-Item $final "$offsite.partial" -Force
        Move-Item "$offsite.partial" $offsite -Force
        Set-Content (Join-Path $OffsiteDir "${db}_latest.txt") "Copy of $([IO.Path]::GetFileName($final)), made $(Get-Date -Format 'yyyy-MM-dd HH:mm')" -Encoding utf8
        Write-Log "Copied to $offsite"
    }
    exit 0
}
catch {
    Write-Log "BACKUP FAILED: $_"
    if ($partial -and (Test-Path $partial)) { Remove-Item $partial -ErrorAction SilentlyContinue }
    exit 1
}
