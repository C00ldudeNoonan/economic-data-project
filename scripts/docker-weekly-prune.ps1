# Docker Weekly Prune Script
# Removes unused images, containers, networks, volumes, and build cache.
# Schedule this via Task Scheduler to run weekly.

$logFile = "$env:LOCALAPPDATA\Docker\prune-log.txt"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

try {
    docker info 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        "$timestamp - Docker not running, skipping prune" | Out-File -Append $logFile
        exit 0
    }

    $before = docker system df --format "{{.Size}}" 2>$null
    "$timestamp - Before prune: $before" | Out-File -Append $logFile

    # Remove containers stopped more than 24h ago, unused images, build cache
    docker system prune -a -f --filter "until=24h" 2>$null
    docker volume prune -a -f 2>$null
    docker builder prune -a -f --filter "until=72h" 2>$null

    $after = docker system df --format "{{.Size}}" 2>$null
    "$timestamp - After prune: $after" | Out-File -Append $logFile
} catch {
    "$timestamp - Error: $_" | Out-File -Append $logFile
}
