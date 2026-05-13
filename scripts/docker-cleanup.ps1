#Requires -Version 5.1
<#
.SYNOPSIS
    Cleans up Docker Desktop disk usage on Windows.

.DESCRIPTION
    Docker Desktop stores all data in a single WSL2 virtual disk (docker_data.vhdx).
    This disk grows as you pull images, create volumes, and build layers — but it
    NEVER shrinks automatically, even after docker system prune.

    This script handles both phases:
      Phase 1: Docker prune (requires Docker Desktop running)
      Phase 2: Compact the VHDX to reclaim freed space (requires Docker Desktop stopped)

.NOTES
    Run from an elevated (Administrator) PowerShell for VHDX compaction.
    The VHDX path assumes default Docker Desktop WSL2 backend on Windows.
#>

param(
    [switch]$SkipPrune,
    [switch]$SkipCompact,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$VhdxPath = "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx"

function Write-Step($msg) { Write-Host "`n>> $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "   $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "   $msg" -ForegroundColor Yellow }
function Write-Err($msg)  { Write-Host "   $msg" -ForegroundColor Red }

function Get-VhdxSizeGB {
    if (Test-Path $VhdxPath) {
        return [math]::Round((Get-Item $VhdxPath).Length / 1GB, 1)
    }
    return 0
}

# ---------------------------------------------------------------------------
# Show current state
# ---------------------------------------------------------------------------
Write-Step "Current Docker disk usage"
$initialSizeGB = Get-VhdxSizeGB
if ($initialSizeGB -gt 0) {
    Write-Ok "VHDX size: $initialSizeGB GB ($VhdxPath)"
} else {
    Write-Err "VHDX not found at $VhdxPath"
    exit 1
}

# ---------------------------------------------------------------------------
# Phase 1: Docker prune (needs Docker running)
# ---------------------------------------------------------------------------
if (-not $SkipPrune) {
    Write-Step "Phase 1: Docker prune (requires Docker Desktop running)"

    $dockerRunning = $null -ne (Get-Process "Docker Desktop" -ErrorAction SilentlyContinue)
    if (-not $dockerRunning) {
        Write-Warn "Docker Desktop is not running."
        Write-Warn "Start Docker Desktop, then re-run this script."
        Write-Warn "Or use -SkipPrune to go straight to VHDX compaction."
        exit 1
    }

    # Wait for Docker engine to be responsive
    Write-Ok "Waiting for Docker engine..."
    $ready = $false
    for ($i = 0; $i -lt 12; $i++) {
        try {
            docker info 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        } catch {}
        Start-Sleep -Seconds 5
    }
    if (-not $ready) {
        Write-Err "Docker engine not responding after 60s. Start Docker Desktop first."
        exit 1
    }

    # Show what's using space
    Write-Ok "Current Docker disk breakdown:"
    docker system df

    if ($DryRun) {
        Write-Warn "[DRY RUN] Would run: docker system prune -a --volumes -f"
        Write-Warn "[DRY RUN] Would run: docker builder prune -a -f"
    } else {
        Write-Ok "Removing all unused containers, networks, images..."
        docker system prune -a -f

        Write-Ok "Removing all unused volumes..."
        docker volume prune -a -f

        Write-Ok "Removing all build cache..."
        docker builder prune -a -f

        Write-Ok "Docker disk after prune:"
        docker system df
    }

    $afterPruneSizeGB = Get-VhdxSizeGB
    Write-Ok "VHDX size after prune: $afterPruneSizeGB GB (freed inside container, not yet on host)"
    Write-Warn "The VHDX file does NOT shrink after prune. Phase 2 compacts it."
}

# ---------------------------------------------------------------------------
# Phase 2: Compact VHDX (needs Docker stopped)
# ---------------------------------------------------------------------------
if (-not $SkipCompact) {
    Write-Step "Phase 2: Compact VHDX (requires Docker Desktop stopped)"

    $dockerProcess = Get-Process "Docker Desktop" -ErrorAction SilentlyContinue
    if ($dockerProcess) {
        if ($DryRun) {
            Write-Warn "[DRY RUN] Would stop Docker Desktop"
        } else {
            Write-Ok "Shutting down Docker Desktop..."
            & "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe" --quit 2>$null

            # Wait for Docker processes to exit
            Write-Ok "Waiting for Docker to fully stop..."
            for ($i = 0; $i -lt 30; $i++) {
                $still = Get-Process "Docker Desktop" -ErrorAction SilentlyContinue
                if (-not $still) { break }
                Start-Sleep -Seconds 2
            }

            # Also wait for WSL docker-desktop distro to stop
            Start-Sleep -Seconds 5
            wsl --shutdown 2>$null
            Start-Sleep -Seconds 3
        }
    }

    # Verify Docker is stopped
    $dockerProcess = Get-Process "Docker Desktop" -ErrorAction SilentlyContinue
    if ($dockerProcess -and -not $DryRun) {
        Write-Err "Docker Desktop is still running. Please close it manually, then re-run with -SkipPrune."
        exit 1
    }

    $beforeCompactGB = Get-VhdxSizeGB
    Write-Ok "VHDX size before compaction: $beforeCompactGB GB"

    if ($DryRun) {
        Write-Warn "[DRY RUN] Would compact $VhdxPath using diskpart"
    } else {
        Write-Ok "Compacting VHDX (this may take several minutes)..."

        # Build diskpart script
        $diskpartScript = @"
select vdisk file="$VhdxPath"
attach vdisk readonly
compact vdisk
detach vdisk
exit
"@
        $scriptFile = "$env:TEMP\docker_compact_vhdx.txt"
        $diskpartScript | Out-File -FilePath $scriptFile -Encoding ASCII

        # Run diskpart (requires admin)
        $isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
        if (-not $isAdmin) {
            Write-Err "VHDX compaction requires Administrator privileges."
            Write-Err "Re-run this script as Administrator, or run these commands manually:"
            Write-Host ""
            Write-Host "  diskpart" -ForegroundColor White
            Write-Host "  select vdisk file=`"$VhdxPath`"" -ForegroundColor White
            Write-Host "  attach vdisk readonly" -ForegroundColor White
            Write-Host "  compact vdisk" -ForegroundColor White
            Write-Host "  detach vdisk" -ForegroundColor White
            Write-Host "  exit" -ForegroundColor White
            Write-Host ""
            Remove-Item $scriptFile -ErrorAction SilentlyContinue
            exit 1
        }

        diskpart /s $scriptFile
        Remove-Item $scriptFile -ErrorAction SilentlyContinue

        $afterCompactGB = Get-VhdxSizeGB
        $savedGB = [math]::Round($beforeCompactGB - $afterCompactGB, 1)
        Write-Ok "VHDX size after compaction: $afterCompactGB GB"
        Write-Ok "Reclaimed: $savedGB GB"
    }
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
Write-Step "Summary"
$finalSizeGB = Get-VhdxSizeGB
$totalSavedGB = [math]::Round($initialSizeGB - $finalSizeGB, 1)
Write-Ok "Initial VHDX size:  $initialSizeGB GB"
Write-Ok "Final VHDX size:    $finalSizeGB GB"
if ($totalSavedGB -gt 0) {
    Write-Ok "Total reclaimed:    $totalSavedGB GB"
} elseif ($DryRun) {
    Write-Warn "Dry run — no changes made"
} else {
    Write-Warn "No space reclaimed (VHDX may already be compact)"
}

Write-Host ""
Write-Ok "Done! You can restart Docker Desktop now."
