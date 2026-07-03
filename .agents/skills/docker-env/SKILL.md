---
name: docker-env
description: Standardize Docker/Dagster environment debugging and recovery workflows.
version: 1.0.0
tags: [docker, dagster, debugging]
---

# Docker Environment Skill

Use this skill for container health checks, logs, and safe restarts. Prefer the least destructive command that answers the question.

## Common Commands

- **Status**
  - `docker compose ps -a`

- **Logs**
  - `docker compose logs <service> --tail=200`
  - `docker compose logs -f <service>` (stream)

- **Restart (service only)**
  - `docker compose restart <service>`

- **Rebuild (non-destructive)**
  - `docker compose up --build -d`

## Destructive Operations (ask before running)

- **Cleanup**
  - `docker system prune -f`

- **Full reset**
  - `docker compose down -v`
  - `docker compose up --build -d`

## Compose Files

- Default: `docker-compose.yml`
- Local dev override: `docker-compose.local.yml`
  - Use `docker compose -f docker-compose.yml -f docker-compose.local.yml ...` if needed

## Guidance

- Prefer targeted restarts before rebuilding or resetting volumes.
- Always capture logs before teardown for root-cause analysis.
