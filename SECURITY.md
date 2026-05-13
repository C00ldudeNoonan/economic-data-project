# Security Policy

## Reporting a Vulnerability

If you discover a security issue, **please do not open a public issue**. Instead, report it privately via GitHub's [Security Advisories](https://github.com/C00ldudeNoonan/economic-data-project/security/advisories/new) feature.

You can expect an initial response within a few days. If the issue is confirmed, a fix will be prioritized and you'll be credited in the release notes (unless you'd prefer to stay anonymous).

## Scope

This project is a personal portfolio data platform. It is not run as a hosted service, so the main concerns are:

- Hardcoded secrets, tokens, or credentials in the repo
- Insecure default configurations in example files (`.env.example`, `docker-compose.yml`, etc.)
- Dependencies with known vulnerabilities

Issues with third-party APIs, services, or dependencies should be reported to those projects directly.
