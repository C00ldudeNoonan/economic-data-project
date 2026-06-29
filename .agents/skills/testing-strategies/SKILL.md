---
name: testing-strategies
description: Comprehensive testing patterns and examples for Dagster, dbt, FastAPI, and React in the economic data project.
---

# Testing Strategies for Economic Data Project

This skill provides comprehensive testing patterns and examples for all components of the economic data project: Dagster (orchestration), dbt (transformations), FastAPI (backend), and React (frontend).

## Quick Reference

| Component | Framework | Run Command | Location |
|-----------|-----------|-------------|----------|
| Dagster | pytest | `make test` or `cd macro_agents && uv run pytest tests/ -v` | `macro_agents/tests/` |
| dbt | dbt test | `dbt test` or `dbt test --select model_name` | `dbt_project/tests/` + schema.yml |
| Backend API | pytest | `cd api && PYTHONPATH=.. uv run pytest tests/ -v` | `api/tests/` |
| Frontend | vitest | `cd frontend && npm test -- --run` | `frontend/tests/` |

---

## 1. Dagster Testing

### Test Categories

1. **Unit Tests** - Test individual resources and functions
2. **Integration Tests** - Test Dagster definitions load correctly
3. **Schedule/Sensor Tests** - Test automation configuration
4. **Data Validation Tests** - Test data schemas and integrity

### Resource Unit Tests

Test Dagster resources in isolation with mocked dependencies:

```python
import pytest
from unittest.mock import Mock, patch
from macro_agents.defs.resources.motherduck import MotherDuckResource

class TestMotherDuckResource:
    def test_initialization_dev_environment(self):
        resource = MotherDuckResource(
            md_token="test_token",
            environment="dev",
            local_path="test.duckdb"
        )
        assert resource.environment == "dev"
        assert resource.db_connection == "test.duckdb"

    def test_initialization_prod_environment(self):
        resource = MotherDuckResource(
            md_token="test_token",
            environment="prod",
            md_database="test_db"
        )
        assert resource.environment == "prod"
        assert resource.db_connection == "md:?motherduck_token=test_token"
```

### Testing with Temporary Database

Use `tempfile` for tests requiring actual database operations:

```python
import tempfile
import os
import polars as pl

def test_table_exists(self):
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
        resource = MotherDuckResource(
            md_token="test_token",
            environment="dev",
            local_path=tmp_file.name
        )

        assert not resource.table_exists("non_existent_table")

        test_df = pl.DataFrame({"id": [1, 2, 3]})
        resource.drop_create_duck_db_table("test_table", test_df)
        assert resource.table_exists("test_table")

        os.unlink(tmp_file.name)
```

### Integration Tests - Definitions Load

Verify all Dagster definitions load without errors:

```python
from macro_agents.definitions import defs

class TestDagsterDefinitions:
    def test_definitions_load(self):
        assert defs is not None
        assert len(defs.assets) > 0
        assert len(defs.resources) > 0

    def test_all_resources_are_configurable(self):
        for resource_key, resource in defs.resources.items():
            assert hasattr(resource, "model_config")
            assert hasattr(type(resource), "model_fields")
```

### Schedule and Sensor Tests

Test schedule configuration and timing:

```python
from macro_agents.defs.replication import weekly_replication_schedule

class TestSchedules:
    def test_weekly_replication_schedule_configuration(self):
        assert weekly_replication_schedule.name == "weekly_replication_schedule"
        assert weekly_replication_schedule.cron_schedule == "0 2 * * 0"
        assert weekly_replication_schedule.execution_timezone == "America/New_York"

    def test_schedule_timing_consistency(self):
        replication_hour = 2  # 2 AM EST
        assert replication_hour <= 6, "Weekly replication should run early in the morning"
```

### Mocking External Services

Use `patch` for external API calls and credentials:

```python
from unittest.mock import Mock, patch

def test_sling_resource_setup_with_credentials(self):
    test_creds = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
    }
    creds_json = json.dumps(test_creds)
    mock_context = Mock()
    mock_context.log = Mock()

    with patch.dict(os.environ, {"SLING_GOOGLE_APPLICATION_CREDENTIALS": creds_json}):
        with patch("macro_agents.defs.replication.sling.SlingConnectionResource") as mock_conn:
            resource = SlingResourceWithCredentials()
            resource.setup_for_execution(mock_context)
            assert hasattr(resource, "_sling_resource")
```

### Skip Tests When Environment Not Available

Use `pytest.mark.skipif` for optional integration tests:

```python
@pytest.mark.skipif(
    not os.getenv("MOTHERDUCK_TOKEN"),
    reason="MOTHERDUCK_TOKEN not set"
)
def test_motherduck_connection_parameters(self):
    token = os.getenv("MOTHERDUCK_TOKEN")
    assert len(token) > 0
```

---

## 2. dbt Testing

### Test Types

1. **Schema Tests** - Defined in `schema.yml` files (not_null, unique, accepted_values)
2. **Data Tests** - Custom SQL in `tests/` folder
3. **dbt Project Validation** - Python tests that run dbt commands

### Schema Tests in schema.yml

```yaml
version: 2

models:
  - name: major_indicies_summary
    description: "Performance analysis of major stock market indices"
    columns:
      - name: symbol
        tests:
          - not_null
          - unique
      - name: time_period
        tests:
          - not_null
          - accepted_values:
              values: ['12_weeks', '6_months', '1_year', '5_years']
      - name: total_return_pct
        tests:
          - not_null
```

### Custom Data Tests

Create SQL files in `dbt_project/tests/` that return failing rows:

```sql
-- tests/test_forward_returns_not_zero.sql
{{ config(severity='warn') }}

{% set models_to_test = [
    'currency_analysis_return',
    'major_indicies_analysis_return',
] %}

{% for model in models_to_test %}
    SELECT
        symbol,
        month_date,
        pct_change_q1_forward,
        'zero_forward_return' as issue_type
    FROM {{ ref(model) }}
    WHERE ABS(pct_change_q1_forward) < 0.01
        AND pct_change_q1_forward IS NOT NULL

    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
```

### Data Completeness Tests

```sql
-- tests/test_weekly_data_completeness.sql
WITH weekly_counts AS (
    SELECT
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_us_sectors') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
    GROUP BY DATE_TRUNC('week', date)
)
SELECT week_start
FROM weekly_counts
WHERE record_count < 10
```

### Python Tests for dbt Validation

Test dbt project from Python (runs in Dagster test suite):

```python
import subprocess
from pathlib import Path

class TestDbtProjectValidation:
    @pytest.fixture
    def dbt_project_dir(self):
        return Path(__file__).parent.parent.parent / "dbt_project"

    def test_dbt_parse_succeeds(self, dbt_project_dir):
        original_cwd = os.getcwd()
        try:
            os.chdir(dbt_project_dir)
            result = subprocess.run(
                ["dbt", "parse", "--target", "local"],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                pytest.fail(f"dbt parse failed:\n{result.stderr}")
        finally:
            os.chdir(original_cwd)

    def test_dbt_list_models_succeeds(self, dbt_project_dir):
        original_cwd = os.getcwd()
        try:
            os.chdir(dbt_project_dir)
            result = subprocess.run(
                ["dbt", "list", "--target", "local", "--resource-type", "model"],
                capture_output=True,
                text=True,
            )
            models = [l.strip() for l in result.stdout.split("\n") if l.strip()]
            assert len(models) > 0, "No models found"
        finally:
            os.chdir(original_cwd)
```

---

## 3. FastAPI Backend Testing

### Test Setup (conftest.py)

```python
import pytest
from fastapi.testclient import TestClient
from api.main import app

@pytest.fixture
def client():
    return TestClient(app)

@pytest.fixture
def temp_duckdb_file():
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
        yield tmp_file.name
        if os.path.exists(tmp_file.name):
            os.unlink(tmp_file.name)

@pytest.fixture
def mock_motherduck_resource(temp_duckdb_file):
    return MotherDuckResource(
        md_token="test_token",
        environment="dev",
        local_path=temp_duckdb_file,
    )
```

### API Endpoint Tests

```python
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_get_market_summaries():
    response = client.get("/api/markets/summary")
    assert response.status_code in [200, 500]  # 500 if no DB
    if response.status_code == 200:
        data = response.json()
        assert "results" in data
        assert "count" in data
        assert isinstance(data["results"], list)

def test_get_market_summaries_with_category():
    response = client.get("/api/markets/summary?category=sector")
    assert response.status_code in [200, 500]

def test_get_market_summaries_invalid_limit():
    response = client.get("/api/markets/summary?limit=0")
    assert response.status_code == 400

    response = client.get("/api/markets/summary?limit=1001")
    assert response.status_code == 400
```

### Testing Validation Errors

```python
def test_invalid_limit_returns_400():
    response = client.get("/api/markets/summary?limit=0")
    assert response.status_code == 400
    assert "detail" in response.json()
```

### Service Layer Tests

```python
def test_data_service_initialization():
    service = DataService()
    assert service.motherduck is not None

def test_get_market_summaries_empty():
    service = DataService()
    results = service.get_market_summaries()
    assert isinstance(results, list)
```

---

## 4. React Frontend Testing

### Test Setup (tests/setup.ts)

```typescript
import { expect, afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';
import * as matchers from '@testing-library/jest-dom/matchers';

expect.extend(matchers);

afterEach(() => {
  cleanup();
});
```

### Component Tests with Providers

Wrap components with required providers:

```tsx
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '../theme/ThemeContext';

function renderWithProviders(component: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>{component}</ThemeProvider>
    </QueryClientProvider>
  );
}
```

### Mocking API Hooks

```tsx
import * as apiHooks from '../hooks/useApi';

vi.mock('../hooks/useApi');

describe('Dashboard', () => {
  it('should render loading state', () => {
    vi.mocked(apiHooks.useLatestEconomyState).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as ReturnType<typeof apiHooks.useLatestEconomyState>);

    renderWithProviders(<Dashboard />);
    expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);
  });

  it('should render dashboard with data', async () => {
    const mockData = {
      analysis_date: '2024-01-01',
      model_name: 'test-model',
      analysis_content: 'Test content',
    };

    vi.mocked(apiHooks.useLatestEconomyState).mockReturnValue({
      data: mockData,
      isLoading: false,
      error: null,
    } as ReturnType<typeof apiHooks.useLatestEconomyState>);

    renderWithProviders(<Dashboard />);
    await waitFor(() => {
      expect(screen.getByText('Latest Economy State')).toBeInTheDocument();
    });
  });
});
```

### Testing User Interactions

```tsx
import { fireEvent } from '@testing-library/react';

it('should call onRangeChange when a button is clicked', () => {
  const onRangeChange = vi.fn();
  render(<TimeRangeSelector selectedRange="1Y" onRangeChange={onRangeChange} />);

  fireEvent.click(screen.getByText('2Y'));
  expect(onRangeChange).toHaveBeenCalledWith('2Y');
});
```

### Testing with Theme Provider

```tsx
import { ThemeProvider } from '../../theme/ThemeContext';

function renderWithTheme(component: React.ReactElement) {
  return render(<ThemeProvider>{component}</ThemeProvider>);
}

describe('TimeSeriesChart', () => {
  const mockData = [
    { date: '2024-01-01', value: 100 },
    { date: '2024-01-02', value: 105 },
  ];

  it('should render chart with data', () => {
    renderWithTheme(<TimeSeriesChart data={mockData} title="Test Chart" />);
    expect(screen.getByText('Test Chart')).toBeInTheDocument();
  });

  it('should render chart without title', () => {
    const { container } = renderWithTheme(<TimeSeriesChart data={mockData} />);
    expect(container.querySelector('h3')).not.toBeInTheDocument();
  });
});
```

### Testing Async Effects and Cleanup

Test components with useEffect cleanup to ensure no memory leaks:

```tsx
import { render, screen, waitFor, act } from '@testing-library/react';

describe('DataLoader (async useEffect)', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should not update state after unmount', async () => {
    const consoleSpy = vi.spyOn(console, 'error');

    const mockFetch = vi.fn().mockImplementation(
      () => new Promise(resolve => setTimeout(() => resolve({ data: 'test' }), 1000))
    );
    vi.mocked(api.fetchData).mockImplementation(mockFetch);

    const { unmount } = renderWithProviders(<DataLoader />);

    // Unmount before the fetch resolves
    unmount();

    // Fast-forward past the timeout
    await act(async () => {
      vi.advanceTimersByTime(1500);
    });

    // Should not have any "Can't perform state update on unmounted component" warnings
    expect(consoleSpy).not.toHaveBeenCalled();
  });

  it('should cancel previous request when dependencies change', async () => {
    const abortSpy = vi.fn();
    global.AbortController = vi.fn().mockImplementation(() => ({
      signal: {},
      abort: abortSpy,
    }));

    const { rerender } = renderWithProviders(<DataLoader id="1" />);

    // Trigger a re-render with new props before first fetch completes
    rerender(
      <QueryClientProvider client={queryClient}>
        <DataLoader id="2" />
      </QueryClientProvider>
    );

    // First request should have been aborted
    expect(abortSpy).toHaveBeenCalled();
  });

  it('should handle loading, success, and error states', async () => {
    vi.mocked(api.fetchData).mockResolvedValueOnce({ items: [] });

    renderWithProviders(<DataLoader />);

    // Loading state
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();

    await waitFor(() => {
      // Success state
      expect(screen.queryByTestId('loading-spinner')).not.toBeInTheDocument();
      expect(screen.getByText('No items found')).toBeInTheDocument();
    });
  });
});
```

### Testing Custom Hooks with Async Operations

```tsx
import { renderHook, waitFor, act } from '@testing-library/react';

describe('useAsyncData hook', () => {
  it('should return data after fetch completes', async () => {
    vi.mocked(api.getData).mockResolvedValueOnce({ result: 'test' });

    const { result } = renderHook(() => useAsyncData('test-id'));

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
      expect(result.current.data).toEqual({ result: 'test' });
    });
  });

  it('should handle errors gracefully', async () => {
    vi.mocked(api.getData).mockRejectedValueOnce(new Error('Network error'));

    const { result } = renderHook(() => useAsyncData('test-id'));

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
      expect(result.current.error).toBe('Network error');
    });
  });

  it('should cleanup on unmount', async () => {
    const mockAbort = vi.fn();
    global.AbortController = vi.fn().mockImplementation(() => ({
      signal: { aborted: false },
      abort: mockAbort,
    }));

    const { unmount } = renderHook(() => useAsyncData('test-id'));

    unmount();

    expect(mockAbort).toHaveBeenCalled();
  });
});
```

---

## 5. Running Tests

### All Tests (via Makefile)

```bash
make test
```

### Dagster Tests

```bash
cd macro_agents
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_resources.py -v

# Run specific test class
uv run pytest tests/test_resources.py::TestMotherDuckResource -v

# Run with coverage
uv run pytest tests/ --cov=macro_agents --cov-report=html
```

### dbt Tests

```bash
cd dbt_project

# Run all tests
dbt test

# Run tests for specific model
dbt test --select major_indicies_summary

# Run data tests only
dbt test --select test_type:data

# Run schema tests only
dbt test --select test_type:schema
```

### API Tests

```bash
cd api
PYTHONPATH=.. uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_markets.py -v
```

### Frontend Tests

```bash
cd frontend

# Run tests (watch mode)
npm test

# Run tests once
npm test -- --run

# Run with UI
npm run test:ui
```

---

## 6. Best Practices

### General

1. **Test one thing per test** - Each test should verify a single behavior
2. **Use descriptive test names** - Name should explain what is being tested
3. **Arrange-Act-Assert** - Structure tests clearly
4. **Clean up resources** - Use fixtures with cleanup for temp files/databases

### Dagster

1. **Mock external services** - Never call real APIs in unit tests
2. **Test resource initialization** - Verify both dev and prod configurations
3. **Test definitions load** - Catch import/config errors early
4. **Use `pytest.mark.skipif`** - Skip integration tests when credentials unavailable

### dbt

1. **Add schema tests for every model** - At minimum: not_null on key columns
2. **Use custom tests for business logic** - Data completeness, value ranges
3. **Set severity levels** - Use `severity='warn'` for non-blocking issues
4. **Test incrementally** - Run `dbt test --select model_name` during development

### FastAPI

1. **Test both success and error cases** - Include validation errors
2. **Use TestClient** - Simulates HTTP requests without running server
3. **Accept multiple status codes** - `[200, 500]` for endpoints that depend on DB state
4. **Test with fixtures** - Use `conftest.py` for shared test setup

### React

1. **Mock API hooks** - Don't make real API calls in tests
2. **Use providers** - Wrap with QueryClientProvider, ThemeProvider
3. **Test loading/error states** - Cover all UI states
4. **Prefer user-centric queries** - Use `getByText`, `getByRole` over `querySelector`
