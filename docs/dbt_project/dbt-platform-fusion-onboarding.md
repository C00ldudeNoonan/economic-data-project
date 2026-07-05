# dbt Platform + Fusion Onboarding

Issue: [#82](https://github.com/C00ldudeNoonan/economic-data-project/issues/82)

This runbook captures the repo-side and dbt Platform setup required to run the
BigQuery dbt project on the Fusion engine.

## Current State

- Warehouse: BigQuery.
- dbt engine: Fusion-compatible project metadata parses locally.
- Project path: `dbt_project/`.
- Profile: `econ_database`.
- Model count validated by Wizard index: 132 models, 2 seeds, 41 sources, 385 tests.
- Iceberg support: `catalogs.yml` defines the `iceberg_catalog` BigLake write integration.

## dbt Platform Project

Create one dbt Platform project connected to this GitHub repository:

| Setting | Value |
| --- | --- |
| Repository | `C00ldudeNoonan/economic-data-project` |
| Project subdirectory | `dbt_project` |
| Default branch | `main` |
| Engine | Fusion |
| Adapter | BigQuery |

## BigQuery Connection

Configure a BigQuery connection with a service account that can create jobs and
write to the economics datasets.

Required permissions:

- `roles/bigquery.jobUser` on the project.
- `roles/bigquery.dataEditor` on target datasets.
- BigLake/Iceberg permissions for the GCS bucket used by `ICEBERG_BUCKET_NAME`.

Required environment variables:

| Variable | Purpose |
| --- | --- |
| `BIGQUERY_PROJECT` | BigQuery project id. Falls back to `BIGQUERY_PROJECT_ID` locally. |
| `BIGQUERY_LOCATION` | BigQuery location, default `US`. |
| `ICEBERG_BUCKET_NAME` | GCS bucket backing BigLake Iceberg tables. |
| `DBT_TARGET` | `dev`, `staging`, or `prod`. |

## Local ADC Setup

Local dbt runs use Application Default Credentials through `method: oauth` in
`dbt_project/profiles.yml`. No service-account keyfile is required for the
standard path.

For user ADC:

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project "$BIGQUERY_PROJECT"
```

For service-account ADC via impersonation:

```bash
gcloud auth application-default login \
  --impersonate-service-account service-account@project.iam.gserviceaccount.com
gcloud auth application-default set-quota-project "$BIGQUERY_PROJECT"
```

The impersonating user needs `roles/iam.serviceAccountTokenCreator` on the
service account. The service account still needs the BigQuery permissions listed
above.

## Environments

Configure three environments:

| Environment | Target | Purpose |
| --- | --- | --- |
| Development | `dev` | Developer validation with `_dev` dataset suffixes. |
| Staging | `staging` | Pre-production validation with `_staging` dataset suffixes. |
| Production | `prod` | Scheduled production builds in canonical datasets. |

## Jobs

Recommended dbt Platform jobs:

| Job | Environment | Trigger | Command |
| --- | --- | --- | --- |
| PR Check | Development | Pull request | `dbt parse && dbt build --select state:modified+ --defer --state prod` |
| Production Build | Production | Schedule | `dbt build` |
| Source Freshness | Production | Schedule before build | `dbt source freshness` |

The PR job should use deferral against the latest production state so pull
requests only build modified nodes and their downstream dependents.

## CI/CD Ownership

dbt Platform owns dbt-specific CI/CD. GitHub Actions should validate Python,
Dagster code loading, and unit tests, but should not be the primary place where
dbt builds run.

| System | Responsibility |
| --- | --- |
| dbt Platform CI | Pull-request dbt validation with Fusion, deferral, and BigQuery credentials. |
| dbt Platform scheduled jobs | Production `dbt build` and `dbt source freshness`. |
| GitHub Actions | Ruff, typecheck, pytest, and Dagster definition loading. |
| Dagster | Asset orchestration and optional dbt Platform ad hoc run triggering/observation. |

GitHub branch protection should require:

- dbt Platform PR check for modified dbt assets.
- `Dagster CI / Test Dagster`.

GitHub Actions should have these repository secrets so `dg check defs` can load
dbt asset specs from dbt Platform instead of compiling the local dbt project:

| GitHub secret/variable | Purpose |
| --- | --- |
| `DBT_PLATFORM_ACCOUNT_ID` | dbt Platform account id. |
| `DBT_PLATFORM_PROJECT_ID` | dbt Platform project id. |
| `DBT_PLATFORM_ENVIRONMENT_ID` | dbt Platform environment id for CI/Dagster ad hoc runs. |
| `DBT_PLATFORM_TOKEN` | dbt Platform service token. |
| `DBT_PLATFORM_ACCESS_URL` | Optional repository variable; defaults to `https://cloud.getdbt.com`. |
| `DBT_PLATFORM_ADHOC_JOB_NAME` | Optional repository variable; defaults to `economic-data-dagster-ci`. |

If `DBT_PLATFORM_TOKEN` is not available, the GitHub workflow falls back to
`DBT_ORCHESTRATION_MODE=local` so forks and local smoke checks can still validate
Dagster definitions without dbt Platform credentials.

## Fusion Compatibility Results

Local Wizard validation after issue setup:

- `dbt parse` succeeds with only `dbt1700: --use-colors is no longer supported`.
- `dbt compile` reaches BigQuery adapter execution, then stops without local ADC credentials.
- YAML schema blockers were fixed by:
  - moving source freshness settings under `config:`;
  - moving generic test parameters under `arguments:`;
  - updating `catalogs.yml` to the Fusion BigLake catalog schema.
- BigQuery auth remains ADC-based through `method: oauth`; local compile requires
  `gcloud auth application-default login` or service-account ADC impersonation.

## Dagster Orchestration Decision

Keep Dagster as the asset orchestrator. The Dagster integration supports two
execution modes:

| Mode | `DBT_ORCHESTRATION_MODE` | Behavior |
| --- | --- | --- |
| Local CLI | `local` | Uses `DbtCliResource` and the bundled `dbt_project/`. |
| dbt Platform | `dbt_platform` | Uses `DbtCloudWorkspace` to load dbt Platform artifacts and launch ad hoc dbt Platform runs. |
| dbt Platform observe-only | `dbt_platform` + `DBT_PLATFORM_OBSERVE_ONLY=true` | Loads dbt Platform asset specs and polls completed dbt Platform jobs, but does not register Dagster dbt jobs or launch ad hoc dbt Platform runs. |

Configure these Dagster environment variables after the dbt Platform project and
environment exist:

| Variable | Purpose |
| --- | --- |
| `DBT_ORCHESTRATION_MODE` | Set to `dbt_platform` to switch Dagster from local dbt CLI execution to dbt Platform. |
| `DBT_PLATFORM_ACCOUNT_ID` | dbt Platform account id. |
| `DBT_PLATFORM_PROJECT_ID` | dbt Platform project id. |
| `DBT_PLATFORM_ENVIRONMENT_ID` | dbt Platform environment id Dagster should load and observe. Use the production environment when dbt Platform owns scheduled production builds and freshness jobs. |
| `DBT_PLATFORM_TOKEN` | dbt Platform service token. |
| `DBT_PLATFORM_ACCESS_URL` | dbt Platform URL, default `https://cloud.getdbt.com`. |
| `DBT_PLATFORM_ADHOC_JOB_NAME` | Optional ad hoc job name Dagster creates/uses in dbt Platform. |
| `DBT_PLATFORM_POLL_INTERVAL_SECONDS` | Optional polling interval for external dbt Platform run observations. |
| `DBT_PLATFORM_OBSERVE_ONLY` | Optional strict production mode. Set to `true` when Dagster should observe dbt Platform jobs without exposing Dagster dbt asset jobs. |

## Dagster Observe-Only Mode

Use observe-only mode for production once dbt Platform owns the scheduled dbt
jobs:

```bash
DBT_ORCHESTRATION_MODE=dbt_platform
DBT_PLATFORM_ENVIRONMENT_ID=<production-environment-id>
DBT_PLATFORM_OBSERVE_ONLY=true
```

In this mode Dagster loads dbt model asset specs from the configured dbt
Platform environment and registers the dbt Cloud polling sensor. The sensor
polls completed dbt Platform runs for that project and environment, skips
Dagster-created ad hoc jobs, reads each run's `run_results.json`, and records
model-level Dagster asset materializations. Dagster does not register the
`dbt_*_models_job` jobs in this mode, so scheduled `dbt build` and
`dbt source freshness` runs remain owned by dbt Platform.

Keep observe-only disabled in local development or in a dedicated CI/ad hoc
environment if you still want Dagster to trigger dbt Platform runs.

Cutover path:

1. Keep current `DbtCliResource` orchestration for local and deployment continuity.
2. Create dbt Platform jobs and confirm production artifacts are available.
3. Set `DBT_ORCHESTRATION_MODE=dbt_platform` and configure the `DBT_PLATFORM_*` variables in Dagster.
4. Validate Dagster code location load; Dagster should fetch the manifest from dbt Platform.
5. Materialize a small dbt asset selection from Dagster to confirm ad hoc dbt Platform runs work.
6. For production, set `DBT_PLATFORM_ENVIRONMENT_ID` to the production environment and `DBT_PLATFORM_OBSERVE_ONLY=true`.
7. Confirm the `dbt_cloud_<account>_<project>_<environment>__run_status_sensor` sensor records asset materializations from the dbt Platform production build job.
8. Remove local dbt project bundling and `dbt deps` fallback after the API path is stable.

## Remaining Manual Checklist

- [ ] Create dbt Platform project.
- [ ] Connect GitHub repository.
- [ ] Configure BigQuery service account connection.
- [ ] Create development, staging, and production environments.
- [ ] Add `BIGQUERY_PROJECT`, `BIGQUERY_LOCATION`, `ICEBERG_BUCKET_NAME`, and `DBT_TARGET` variables.
- [ ] Add `DBT_ORCHESTRATION_MODE=dbt_platform` and the required `DBT_PLATFORM_*` variables to Dagster.
- [ ] Import and validate `catalogs.yml`.
- [ ] Configure PR, production build, and source freshness jobs.
- [ ] Add dbt Platform PR check and `Dagster CI / Test Dagster` to GitHub branch protection.
- [ ] Add dbt Platform GitHub repository secrets/variables for Dagster CI.
- [ ] Set `DBT_PLATFORM_OBSERVE_ONLY=true` in production Dagster after dbt Platform schedules are live.
- [ ] Record the production dbt Platform environment id, production build job id, and source freshness job id in deployment notes.
- [ ] Validate Dagster can load dbt Platform asset specs and trigger an ad hoc run.
- [ ] Validate Dagster observes a completed dbt Platform production build and records model-level asset materializations.
