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


class InvestmentRecommendationChartConfig(dg.Config):
    max_charts: int = Field(
        default=3, description="Maximum number of charts to generate"
    )
    months_lookback: int = Field(
        default=36, description="Number of months of history to plot for line charts"
    )
    max_bars: int = Field(
        default=12, description="Maximum number of bars for ranked return charts"
    )
    model_provider: str | None = Field(
        default=None,
        description="LLM provider override: 'openai', 'anthropic', or 'gemini'.",
    )
    model_name: str | None = Field(
        default=None,
        description="LLM model name override. If not provided, uses resource default or env var.",
    )


class InvestmentRecommendationChartSpec(BaseModel):
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
    "major_indices_returns": ChartTemplate(
        key="major_indices_returns",
        title="Major Indices Performance (6M)",
        description="Bar chart of 6-month total return for major indices",
        default_caption="Recent performance across major benchmark indices.",
    ),
    "sector_returns": ChartTemplate(
        key="sector_returns",
        title="Sector Performance (6M)",
        description="Bar chart of 6-month total return for US sectors",
        default_caption="Relative sector strength over the last 6 months.",
    ),
    "yield_curve_spreads": ChartTemplate(
        key="yield_curve_spreads",
        title="Yield Curve Spreads",
        description="Line chart of 10Y-2Y and 10Y-3M Treasury spreads (monthly average)",
        default_caption="Inversions (below 0) historically signal elevated recession risk.",
    ),
    "fci_trend": ChartTemplate(
        key="fci_trend",
        title="Financial Conditions Index",
        description="Line chart of the Financial Conditions Index (FCI) over time",
        default_caption="FCI above 0 suggests expansionary conditions; below 0 indicates tightening.",
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


class InvestmentRecommendationChartSpecSignature(dspy.Signature):
    """Select chart templates that best support the recommendations and add brief captions."""

    recommendations_content: str = dspy.InputField(
        desc="The investment recommendations narrative"
    )
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


class InvestmentRecommendationChartSpecModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generator = dspy.ChainOfThought(InvestmentRecommendationChartSpecSignature)

    def forward(
        self, recommendations_content: str, available_charts: str, max_charts: int
    ):
        return self.generator(
            recommendations_content=recommendations_content,
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


def _parse_chart_specs(
    raw: str, max_charts: int
) -> list[InvestmentRecommendationChartSpec]:
    data = _extract_json_array(raw)
    specs: list[InvestmentRecommendationChartSpec] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        chart_key = item.get("chart_key")
        if not chart_key or chart_key not in CHART_TEMPLATES:
            continue
        specs.append(
            InvestmentRecommendationChartSpec(
                chart_key=chart_key,
                title=item.get("title"),
                caption=item.get("caption"),
            )
        )

    unique_specs: list[InvestmentRecommendationChartSpec] = []
    seen = set()
    for spec in specs:
        if spec.chart_key in seen:
            continue
        seen.add(spec.chart_key)
        unique_specs.append(spec)
        if len(unique_specs) >= max_charts:
            break

    return unique_specs


def _default_specs(max_charts: int) -> list[InvestmentRecommendationChartSpec]:
    defaults = ["major_indices_returns", "sector_returns", "yield_curve_spreads"]
    specs = [InvestmentRecommendationChartSpec(chart_key=key) for key in defaults]
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


def _fetch_market_returns(
    bq: BigQueryWarehouseResource, category: str, max_bars: int
) -> pl.DataFrame:
    query = f"""
    WITH latest AS (
        SELECT MAX(period_end_date) AS max_date
        FROM agent_market_performance
        WHERE market_category = '{category}'
          AND time_period = '6_months'
    )
    SELECT symbol, name, total_return_pct
    FROM agent_market_performance
    WHERE market_category = '{category}'
      AND time_period = '6_months'
      AND period_end_date = (SELECT max_date FROM latest)
    ORDER BY total_return_pct DESC
    LIMIT {max_bars}
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


def _build_ranked_bar_chart(
    df: pl.DataFrame, title: str, x_title: str, theme: ThemeStyle, color_index: int
) -> alt.Chart:
    data = df.with_columns(
        pl.when(pl.col("name").is_not_null())
        .then(pl.col("name"))
        .otherwise(pl.col("symbol"))
        .alias("label")
    ).to_dicts()
    base = _base_chart(data, title, theme)
    return (
        base.mark_bar(color=theme.colors[color_index])
        .encode(
            y=alt.Y("label:N", sort="-x", title=None),
            x=alt.X("total_return_pct:Q", title=x_title),
            tooltip=[
                alt.Tooltip("label:N", title="Asset"),
                alt.Tooltip("total_return_pct:Q", title="6M Return", format=".2f"),
            ],
        )
        .properties(height=320)
    )


def _chart_for_key(
    chart_key: str,
    bq: BigQueryWarehouseResource,
    months: int,
    max_bars: int,
    theme: ThemeStyle,
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
    if chart_key == "major_indices_returns":
        df = _fetch_market_returns(bq, "major_index", max_bars)
        if df.is_empty():
            return None
        return _build_ranked_bar_chart(
            df,
            CHART_TEMPLATES[chart_key].title,
            "6M Total Return (%)",
            theme,
            0,
        )
    if chart_key == "sector_returns":
        df = _fetch_market_returns(bq, "sector", max_bars)
        if df.is_empty():
            return None
        return _build_ranked_bar_chart(
            df,
            CHART_TEMPLATES[chart_key].title,
            "6M Total Return (%)",
            theme,
            1,
        )
    return None


def _inject_chart_tokens(
    recommendations_content: str, manifest: list[dict[str, Any]]
) -> str:
    if not recommendations_content:
        recommendations_content = ""
    if "[[chart:" in recommendations_content:
        return recommendations_content

    lines = ["", "## Charts", ""]
    for entry in manifest:
        chart_id = entry.get("id")
        if chart_id:
            lines.append(f"[[chart:{chart_id}]]")
            lines.append("")
    return recommendations_content.strip() + "\n" + "\n".join(lines)


def _fetch_recommendations_row(
    bq: BigQueryWarehouseResource, run_id: str
) -> dict[str, Any] | None:
    query = """
    SELECT
        analysis_timestamp,
        analysis_date,
        analysis_time,
        model_name,
        personality,
        recommendations_content
    FROM investment_recommendations
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
    description="Generate chart images for investment recommendations and store a chart manifest",
    deps=[dg.AssetKey(["generate_investment_recommendations"])],
)
def generate_investment_recommendation_charts(
    context: dg.AssetExecutionContext,
    config: InvestmentRecommendationChartConfig,
    bq: BigQueryWarehouseResource,
    economic_analysis: EconomicAnalysisResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    recommendations_row = _fetch_recommendations_row(bq, context.run_id)
    if not recommendations_row:
        context.log.warning(
            "No investment_recommendations row found for current run_id. Skipping chart generation."
        )
        return dg.MaterializeResult(metadata={"charts_generated": 0})

    recommendations_content = recommendations_row.get("recommendations_content") or ""

    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    chart_selector = InvestmentRecommendationChartSpecModule()
    available_charts = _available_charts_prompt()
    try:
        chart_result = chart_selector(
            recommendations_content=recommendations_content,
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
    timestamp = recommendations_row.get("analysis_timestamp")
    if isinstance(timestamp, datetime):
        timestamp_str = timestamp.isoformat()
    else:
        timestamp_str = str(timestamp)

    analysis_date = recommendations_row.get("analysis_date") or datetime.now().strftime(
        "%Y-%m-%d"
    )

    for idx, spec in enumerate(specs, start=1):
        template = CHART_TEMPLATES[spec.chart_key]
        chart_id = spec.chart_key
        gcs_paths: dict[str, str] = {}

        for theme_name, theme in THEMES.items():
            chart = _chart_for_key(
                spec.chart_key, md, config.months_lookback, config.max_bars, theme
            )
            if chart is None:
                continue
            try:
                png_bytes = _render_chart_to_png(chart)
            except Exception as exc:
                context.log.warning(
                    f"Failed to render chart {spec.chart_key} for theme {theme_name}: {exc}"
                )
                continue

            gcs_path = f"reports/investment_recommendations/{analysis_date}/{timestamp_str}/{chart_id}__{theme_name}.png"
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

    updated_content = _inject_chart_tokens(recommendations_content, manifest)

    if manifest:
        bq.execute_query(
            "ALTER TABLE investment_recommendations ADD COLUMN IF NOT EXISTS chart_manifest JSON",
            read_only=False,
        )
        update_query = """
        UPDATE investment_recommendations
        SET chart_manifest = CAST(? AS JSON),
            recommendations_content = ?
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
            "gcs_prefix": f"reports/investment_recommendations/{analysis_date}/{timestamp_str}/",
        }
    )
