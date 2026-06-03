import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import altair as alt
import dagster as dg
import dspy
import polars as pl
import vl_convert as vlc
from pydantic import BaseModel, Field

from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
    EconomicAnalysisResource,
)
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

alt.data_transformers.disable_max_rows()


class EconomyStateChartConfig(dg.Config):
    max_charts: int = Field(
        default=3, description="Maximum number of charts to generate"
    )
    months_lookback: int = Field(
        default=36, description="Number of months of history to plot"
    )
    model_provider: str | None = Field(
        default=None,
        description="LLM provider override: 'openai', 'anthropic', or 'gemini'.",
    )
    model_name: str | None = Field(
        default=None,
        description="LLM model name override. If not provided, uses resource default or env var.",
    )


class EconomyStateChartSpec(BaseModel):
    chart_key: str
    title: str | None = None
    caption: str | None = None


@dataclass(frozen=True)
class ChartTemplate:
    key: str
    title: str
    description: str
    default_caption: str


CHART_TEMPLATES: dict[str, ChartTemplate] = {
    "fci_trend": ChartTemplate(
        key="fci_trend",
        title="Financial Conditions Index",
        description="Line chart of the Financial Conditions Index (FCI) over time",
        default_caption="FCI above 0 suggests expansionary conditions; below 0 indicates tightening.",
    ),
    "yield_curve_spreads": ChartTemplate(
        key="yield_curve_spreads",
        title="Yield Curve Spreads",
        description="Line chart of 10Y-2Y and 10Y-3M Treasury spreads (monthly average)",
        default_caption="Inversions (below 0) historically signal elevated recession risk.",
    ),
    "unemployment_rate": ChartTemplate(
        key="unemployment_rate",
        title="Unemployment Rate",
        description="Line chart of the unemployment rate (UNRATE) over time",
        default_caption="Labor market slack indicator based on FRED series UNRATE.",
    ),
    "inflation_cpi": ChartTemplate(
        key="inflation_cpi",
        title="CPI Inflation Index",
        description="Line chart of CPI (CPIAUCSL) over time",
        default_caption="Consumer price index trend for inflation monitoring.",
    ),
    "payrolls": ChartTemplate(
        key="payrolls",
        title="Nonfarm Payrolls",
        description="Line chart of nonfarm payrolls (PAYEMS) over time",
        default_caption="Employment momentum from FRED series PAYEMS.",
    ),
}


@dataclass(frozen=True)
class ThemeStyle:
    name: str
    background: str
    text: str
    text_secondary: str
    grid: str
    colors: list[str]
    font: str


THEMES: dict[str, ThemeStyle] = {
    "light": ThemeStyle(
        name="light",
        background="#ffffff",
        text="#111827",
        text_secondary="#6b7280",
        grid="#e5e7eb",
        colors=["#2563eb", "#7c3aed", "#0891b2", "#059669"],
        font="Inter, Arial, sans-serif",
    ),
    "dark": ThemeStyle(
        name="dark",
        background="#0f172a",
        text="#f8fafc",
        text_secondary="#94a3b8",
        grid="#334155",
        colors=["#3b82f6", "#a78bfa", "#22d3ee", "#34d399"],
        font="Space Grotesk, Inter, sans-serif",
    ),
    "terminal": ThemeStyle(
        name="terminal",
        background="#0a0a0a",
        text="#33ff33",
        text_secondary="#22aa22",
        grid="#1a3a1a",
        colors=["#33ff33", "#ffb000", "#00ffff", "#ff6b9d"],
        font="VT323, monospace",
    ),
    "classic": ThemeStyle(
        name="classic",
        background="#f5f0e1",
        text="#2c1810",
        text_secondary="#5c4a3a",
        grid="#c9b896",
        colors=["#8b4513", "#2f4f4f", "#556b2f", "#8b0000"],
        font="Playfair Display, serif",
    ),
}

DEFAULT_THEME = "terminal"

CHART_STYLE = {
    "title_size": 16,
    "label_size": 11,
    "axis_title_size": 12,
    "legend_label_size": 11,
    "legend_title_size": 12,
    "grid_opacity": 0.6,
}


class EconomyStateChartSpecSignature(dspy.Signature):
    """Select chart templates that best support the analysis and add brief captions."""

    analysis_content: str = dspy.InputField(desc="The economy state analysis narrative")
    available_charts: str = dspy.InputField(
        desc="List of available chart templates with keys and descriptions"
    )
    max_charts: str = dspy.InputField(desc="Maximum number of charts to select")

    chart_specs_json: str = dspy.OutputField(
        desc=(
            "Return a JSON array of objects with fields: chart_key, title, caption. "
            "Use chart_key values exactly from the available list."
        )
    )


class EconomyStateChartSpecModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generator = dspy.ChainOfThought(EconomyStateChartSpecSignature)

    def forward(self, analysis_content: str, available_charts: str, max_charts: int):
        return self.generator(
            analysis_content=analysis_content,
            available_charts=available_charts,
            max_charts=str(max_charts),
        )


def _available_charts_prompt() -> str:
    lines = []
    for template in CHART_TEMPLATES.values():
        lines.append(f"- {template.key}: {template.description}")
    return "\n".join(lines)


def _extract_json_array(raw: str) -> list[dict[str, Any]]:
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if not match:
        return []
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return []


def _parse_chart_specs(raw: str, max_charts: int) -> list[EconomyStateChartSpec]:
    data = _extract_json_array(raw)
    specs: list[EconomyStateChartSpec] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        chart_key = item.get("chart_key")
        if not chart_key or chart_key not in CHART_TEMPLATES:
            continue
        specs.append(
            EconomyStateChartSpec(
                chart_key=chart_key,
                title=item.get("title"),
                caption=item.get("caption"),
            )
        )

    unique_specs: list[EconomyStateChartSpec] = []
    seen = set()
    for spec in specs:
        if spec.chart_key in seen:
            continue
        seen.add(spec.chart_key)
        unique_specs.append(spec)
        if len(unique_specs) >= max_charts:
            break

    return unique_specs


def _default_specs(max_charts: int) -> list[EconomyStateChartSpec]:
    defaults = ["fci_trend", "yield_curve_spreads", "unemployment_rate"]
    specs = [EconomyStateChartSpec(chart_key=key) for key in defaults]
    return specs[:max_charts]


def _fetch_fci_data(bq: BigQueryWarehouseResource, months: int) -> pl.DataFrame:
    query = f"""
    SELECT date, FCI
    FROM agent_financial_conditions_index
    WHERE date IS NOT NULL
    ORDER BY date DESC
    LIMIT {months}
    """
    return bq.execute_query(query, read_only=True)


def _fetch_yield_curve_spreads(bq: BigQueryWarehouseResource, months: int) -> pl.DataFrame:
    query = f"""
    SELECT
        DATE_TRUNC('month', date) AS month,
        AVG(spread_10y_2y) AS spread_10y_2y,
        AVG(spread_10y_3m) AS spread_10y_3m
    FROM agent_treasury_yield_curve_spreads
    WHERE date IS NOT NULL
    GROUP BY 1
    ORDER BY month DESC
    LIMIT {months}
    """
    return bq.execute_query(query, read_only=True)


def _fetch_fred_series(
    bq: BigQueryWarehouseResource, series_code: str, months: int
) -> pl.DataFrame:
    query = f"""
    SELECT month AS date, current_value AS value
    FROM agent_fred_series_latest_aggregates
    WHERE series_code = '{series_code}'
      AND month IS NOT NULL
      AND current_value IS NOT NULL
    ORDER BY month DESC
    LIMIT {months}
    """
    return bq.execute_query(query, read_only=True)


def _base_chart(data: list[dict[str, Any]], title: str, theme: ThemeStyle) -> alt.Chart:
    if not data:
        raise ValueError("Empty dataset")
    return (
        alt.Chart(alt.Data(values=data))
        .properties(
            title=alt.TitleParams(
                text=title,
                anchor="start",
                font=theme.font,
                fontSize=CHART_STYLE["title_size"],
                color=theme.text,
            ),
            width=760,
            height=320,
            background=theme.background,
        )
        .configure_axis(
            labelColor=theme.text_secondary,
            titleColor=theme.text,
            gridColor=theme.grid,
            labelFont=theme.font,
            titleFont=theme.font,
            labelFontSize=CHART_STYLE["label_size"],
            titleFontSize=CHART_STYLE["axis_title_size"],
            gridOpacity=CHART_STYLE["grid_opacity"],
            tickColor=theme.grid,
        )
        .configure_legend(
            labelColor=theme.text_secondary,
            titleColor=theme.text,
            labelFont=theme.font,
            titleFont=theme.font,
            labelFontSize=CHART_STYLE["legend_label_size"],
            titleFontSize=CHART_STYLE["legend_title_size"],
        )
        .configure_view(stroke=theme.grid)
    )


def _render_chart_to_png(chart: alt.Chart) -> bytes:
    spec = json.dumps(chart.to_dict())
    return vlc.vegalite_to_png(spec)


def _build_fci_chart(df: pl.DataFrame, theme: ThemeStyle) -> alt.Chart:
    base = _base_chart(df.to_dicts(), CHART_TEMPLATES["fci_trend"].title, theme)
    return base.mark_line(color=theme.colors[0], strokeWidth=2).encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("FCI:Q", title="FCI"),
    )


def _build_yield_curve_chart(df: pl.DataFrame, theme: ThemeStyle) -> alt.Chart:
    melted = df.unpivot(
        on=["spread_10y_2y", "spread_10y_3m"],
        index=["month"],
        variable_name="series",
        value_name="spread",
    )
    base = _base_chart(
        melted.to_dicts(), CHART_TEMPLATES["yield_curve_spreads"].title, theme
    )
    return base.mark_line(strokeWidth=2).encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("spread:Q", title="Spread"),
        color=alt.Color(
            "series:N",
            scale=alt.Scale(range=theme.colors[:2]),
            legend=alt.Legend(title="Series"),
        ),
    )


def _build_single_series_chart(
    df: pl.DataFrame, title: str, y_title: str, theme: ThemeStyle, color_index: int
) -> alt.Chart:
    base = _base_chart(df.to_dicts(), title, theme)
    return base.mark_line(color=theme.colors[color_index], strokeWidth=2).encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("value:Q", title=y_title),
    )


def _chart_for_key(
    chart_key: str, bq: BigQueryWarehouseResource, months: int, theme: ThemeStyle
) -> alt.Chart | None:
    if chart_key == "fci_trend":
        df = _fetch_fci_data(bq, months)
        if df.is_empty():
            return None
        df = df.sort("date")
        return _build_fci_chart(df, theme)
    if chart_key == "yield_curve_spreads":
        df = _fetch_yield_curve_spreads(bq, months)
        if df.is_empty():
            return None
        df = df.sort("month")
        return _build_yield_curve_chart(df, theme)
    if chart_key == "unemployment_rate":
        df = _fetch_fred_series(bq, "UNRATE", months)
        if df.is_empty():
            return None
        df = df.sort("date")
        return _build_single_series_chart(
            df,
            CHART_TEMPLATES[chart_key].title,
            "Unemployment Rate",
            theme,
            0,
        )
    if chart_key == "inflation_cpi":
        df = _fetch_fred_series(bq, "CPIAUCSL", months)
        if df.is_empty():
            return None
        df = df.sort("date")
        return _build_single_series_chart(
            df,
            CHART_TEMPLATES[chart_key].title,
            "CPI Index",
            theme,
            1,
        )
    if chart_key == "payrolls":
        df = _fetch_fred_series(bq, "PAYEMS", months)
        if df.is_empty():
            return None
        df = df.sort("date")
        return _build_single_series_chart(
            df,
            CHART_TEMPLATES[chart_key].title,
            "Payrolls",
            theme,
            2,
        )
    return None


def _inject_chart_tokens(analysis_content: str, manifest: list[dict[str, Any]]) -> str:
    if not analysis_content:
        analysis_content = ""
    if "[[chart:" in analysis_content:
        return analysis_content

    lines = ["", "## Charts", ""]
    for entry in manifest:
        chart_id = entry.get("id")
        if chart_id:
            lines.append(f"[[chart:{chart_id}]]")
            lines.append("")
    return analysis_content.strip() + "\n" + "\n".join(lines)


def _fetch_analysis_row(bq: BigQueryWarehouseResource, run_id: str) -> dict[str, Any] | None:
    query = """
    SELECT
        analysis_timestamp,
        analysis_date,
        analysis_time,
        model_name,
        personality,
        analysis_content,
        chart_manifest
    FROM economy_state_analysis
    WHERE dagster_run_id = ?
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """
    df = bq.execute_query(query, read_only=True, params=[run_id])
    if df.is_empty():
        return None
    return df.to_dicts()[0]


@dg.asset(
    kinds={"dspy", "duckdb", "gcs"},
    group_name="economic_analysis",
    description="Generate chart images for the economy state analysis and store a chart manifest",
    deps=[dg.AssetKey(["analyze_economy_state"])],
)
def generate_economy_state_charts(
    context: dg.AssetExecutionContext,
    config: EconomyStateChartConfig,
    bq: BigQueryWarehouseResource,
    economic_analysis: EconomicAnalysisResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    analysis_row = _fetch_analysis_row(bq, context.run_id)
    if not analysis_row:
        context.log.warning(
            "No economy_state_analysis row found for current run_id. Skipping chart generation."
        )
        return dg.MaterializeResult(metadata={"charts_generated": 0})

    analysis_content = analysis_row.get("analysis_content") or ""

    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    chart_selector = EconomyStateChartSpecModule()
    available_charts = _available_charts_prompt()
    try:
        chart_result = chart_selector(
            analysis_content=analysis_content,
            available_charts=available_charts,
            max_charts=config.max_charts,
        )
        specs = _parse_chart_specs(chart_result.chart_specs_json, config.max_charts)
    except Exception as exc:
        context.log.warning(f"Chart spec generation failed: {exc}. Using defaults.")
        specs = []

    if not specs:
        specs = _default_specs(config.max_charts)

    manifest: list[dict[str, Any]] = []
    timestamp = analysis_row.get("analysis_timestamp")
    if isinstance(timestamp, datetime):
        timestamp_str = timestamp.isoformat()
    else:
        timestamp_str = str(timestamp)

    analysis_date = analysis_row.get("analysis_date") or datetime.now().strftime(
        "%Y-%m-%d"
    )

    for idx, spec in enumerate(specs, start=1):
        template = CHART_TEMPLATES[spec.chart_key]
        chart_id = spec.chart_key
        gcs_paths: dict[str, str] = {}

        for theme_name, theme in THEMES.items():
            chart = _chart_for_key(spec.chart_key, md, config.months_lookback, theme)
            if chart is None:
                continue
            try:
                png_bytes = _render_chart_to_png(chart)
            except Exception as exc:
                context.log.warning(
                    f"Failed to render chart {spec.chart_key} for theme {theme_name}: {exc}"
                )
                continue

            gcs_path = f"reports/economy_state/{analysis_date}/{timestamp_str}/{chart_id}__{theme_name}.png"
            gcs.upload_bytes(
                gcs_path, png_bytes, content_type="image/png", context=context
            )
            gcs_paths[theme_name] = gcs_path

        if not gcs_paths:
            context.log.warning(
                f"No data available for chart {spec.chart_key}, skipping."
            )
            continue

        manifest.append(
            {
                "id": chart_id,
                "chart_key": spec.chart_key,
                "title": spec.title or template.title,
                "caption": spec.caption or template.default_caption,
                "gcs_path": gcs_paths.get(DEFAULT_THEME)
                or next(iter(gcs_paths.values())),
                "gcs_paths": gcs_paths,
                "order": idx,
            }
        )

    updated_content = _inject_chart_tokens(analysis_content, manifest)

    if manifest:
        update_query = """
        UPDATE economy_state_analysis
        SET chart_manifest = CAST(? AS JSON),
            analysis_content = ?
        WHERE analysis_timestamp = ?
        """
        bq.execute_query(
            update_query,
            read_only=False,
            params=[json.dumps(manifest), updated_content, timestamp_str],
        )

    return dg.MaterializeResult(
        metadata={
            "charts_generated": len(manifest),
            "analysis_timestamp": timestamp_str,
            "chart_keys": ", ".join([item["chart_key"] for item in manifest]),
            "gcs_prefix": f"reports/economy_state/{analysis_date}/{timestamp_str}/",
        }
    )
