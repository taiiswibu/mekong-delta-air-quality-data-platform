from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from air_quality_platform.snowflake import connection


st.set_page_config(
    page_title="Mekong Delta Air Quality Intelligence",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

SUCCESS = "#2EAD73"
WARNING = "#F4B942"
DANGER = "#E75A5A"

POLLUTION_COLORS = {
    "Good": "#2EAD73",
    "Moderate": "#F4B942",
    "Unhealthy for Sensitive Groups": "#F28C45",
    "Unhealthy": "#E75A5A",
    "Very Unhealthy": "#8D5FD3",
    "Hazardous": "#7A2538",
}

PLOT_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
}

st.markdown(
    """
    <style>
      .block-container {
        padding-top: 1.1rem;
        padding-bottom: 2rem;
        max-width: 1500px;
      }
      [data-testid="stSidebar"] {
        border-right: 1px solid rgba(128,128,128,.12);
      }
      [data-testid="stMetric"] {
        border: 1px solid rgba(128,128,128,.16);
        border-radius: 16px;
        padding: 14px 16px;
        background: rgba(128,128,128,.035);
      }
      [data-testid="stMetricValue"] {
        font-size: 1.65rem;
        font-weight: 700;
      }
      .hero {
        padding: 1.1rem 1.3rem;
        border-radius: 18px;
        border: 1px solid rgba(46,173,115,.24);
        background: linear-gradient(
          135deg,
          rgba(46,173,115,.12),
          rgba(79,139,249,.035)
        );
        margin-bottom: 1rem;
      }
      .hero h1 {
        font-size: 2rem;
        margin: 0 0 .25rem 0;
      }
      .hero p {
        margin: 0;
        opacity: .72;
      }
      .section-label {
        font-size: .8rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .08em;
        opacity: .6;
        margin: .4rem 0 .15rem 0;
      }
      .note {
        border-left: 4px solid #2EAD73;
        background: rgba(46,173,115,.075);
        padding: .75rem 1rem;
        border-radius: 10px;
        margin: .5rem 0 1rem 0;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60, show_spinner=False)
def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            return cur.fetch_pandas_all()
        finally:
            cur.close()


def safe_query(
    sql: str,
    params: tuple | None = None,
) -> pd.DataFrame:
    try:
        return query(sql, params)
    except Exception as exc:
        st.warning(f"Data is not ready yet: {exc}")
        return pd.DataFrame()


def header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="hero">
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section(title: str) -> None:
    st.markdown(
        f'<div class="section-label">{title}</div>',
        unsafe_allow_html=True,
    )


def style_fig(
    fig: go.Figure,
    *,
    height: int = 420,
) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=55, b=10),
        hovermode="closest",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        title_font=dict(size=16),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,.12)")
    return fig


def fmt_ts(value) -> str:
    if pd.isna(value):
        return "N/A"
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)


def lag_minutes(value) -> float | None:
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, utc=True)
    return max(
        0.0,
        (pd.Timestamp.now(tz="UTC") - ts).total_seconds() / 60,
    )


# ============================================================
# Sidebar
# ============================================================
st.sidebar.markdown("## 🌿 Mekong AQ Intelligence")
st.sidebar.caption("ĐBSCL • Data Engineering portfolio")

PAGES = {
    "🏠 Mekong Overview": "overview",
    "📍 Province Intelligence": "province",
    "🕒 Spatial & Temporal": "spatial",
    "⚙️ Platform Health": "health",
}

page_label = st.sidebar.radio(
    "Workspace",
    list(PAGES),
    label_visibility="collapsed",
)
page = PAGES[page_label]

st.sidebar.markdown("---")

if st.sidebar.button("↻ Refresh data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption(
    "Kafka • Airflow • Snowflake • dbt • Great Expectations"
)
st.sidebar.caption(
    "Analytics use a 13-province/city Mekong Delta demo geography."
)
st.sidebar.caption(
    "Seed records are synthetic demo data, not official measurements."
)


# ============================================================
# 1) MEKONG OVERVIEW
# ============================================================
if page == "overview":
    header(
        "Mekong Delta Air Quality — Overview",
        "Latest air-quality status, spatial ranking and short-term trends across the 13-province/city demo geography.",
    )

    window = st.segmented_control(
        "Trend window",
        [7, 30, 90],
        default=30,
        format_func=lambda x: f"{x} days",
    ) or 30

    latest = safe_query(
        """
        SELECT
            l.QUERIED_CITY,
            r.CITY_NAME,
            r.REGION,
            r.LAT AS CITY_LAT,
            r.LON AS CITY_LON,
            l.AQI,
            l.PM25,
            l.POLLUTION_LEVEL,
            l.MEASURED_AT
        FROM GOLD.AQI_LATEST_STATUS l
        JOIN CONTROL.CITY_REFERENCE r
          ON r.CITY_SLUG = l.QUERIED_CITY
        ORDER BY l.AQI DESC
        """
    )

    daily = safe_query(
        f"""
        SELECT
            d.DATE,
            d.CITY_NAME,
            r.REGION,
            d.AVG_AQI,
            d.MAX_AQI,
            d.AVG_PM25,
            d.POLLUTION_LEVEL
        FROM GOLD.AQI_DAILY_SUMMARY d
        JOIN CONTROL.CITY_REFERENCE r
          ON r.CITY_NAME = d.CITY_NAME
        ORDER BY d.DATE, d.CITY_NAME
        """
    )

    if latest.empty:
        st.info("No Gold-layer Mekong data is available yet.")
    else:
        for col in ["AQI", "PM25"]:
            latest[col] = pd.to_numeric(
                latest[col],
                errors="coerce",
            )

        valid = latest.dropna(subset=["AQI"])
        worst = valid.iloc[0] if not valid.empty else None
        newest = pd.to_datetime(
            latest["MEASURED_AT"],
            errors="coerce",
        ).max()

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric(
            "Provinces / city",
            f"{latest['CITY_NAME'].nunique()}/13",
        )
        k2.metric(
            "Regional avg AQI",
            f"{valid['AQI'].mean():.1f}"
            if not valid.empty
            else "N/A",
        )
        k3.metric(
            "Highest AQI",
            f"{worst['AQI']:.0f}"
            if worst is not None
            else "N/A",
            worst["CITY_NAME"]
            if worst is not None
            else None,
            delta_color="off",
        )
        k4.metric(
            "Average PM2.5",
            f"{latest['PM25'].mean():.1f}"
            if latest["PM25"].notna().any()
            else "N/A",
        )
        k5.metric(
            "Latest measurement",
            fmt_ts(newest),
        )

        section("Spatial snapshot")
        map_col, rank_col = st.columns(
            [1.7, 1],
            gap="large",
        )

        map_df = latest.dropna(
            subset=["CITY_LAT", "CITY_LON", "AQI"]
        )

        if not map_df.empty:
            fig = px.scatter_map(
                map_df,
                lat="CITY_LAT",
                lon="CITY_LON",
                size="AQI",
                color="AQI",
                hover_name="CITY_NAME",
                hover_data={
                    "REGION": True,
                    "AQI": ":.0f",
                    "PM25": ":.1f",
                    "POLLUTION_LEVEL": True,
                    "MEASURED_AT": True,
                    "CITY_LAT": False,
                    "CITY_LON": False,
                },
                center={"lat": 9.95, "lon": 105.65},
                zoom=6.0,
                height=490,
                color_continuous_scale="RdYlGn_r",
                title="Latest AQI map — Mekong Delta",
            )
            fig.update_layout(
                map_style="open-street-map",
                margin=dict(l=0, r=0, t=55, b=0),
                coloraxis_colorbar=dict(title="AQI"),
            )
            map_col.plotly_chart(
                fig,
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        rank = valid.sort_values("AQI")
        if not rank.empty:
            rank_fig = px.bar(
                rank,
                x="AQI",
                y="CITY_NAME",
                color="AQI",
                orientation="h",
                color_continuous_scale="RdYlGn_r",
                hover_data=[
                    "REGION",
                    "PM25",
                    "POLLUTION_LEVEL",
                ],
                title="Latest AQI ranking",
            )
            rank_fig.update_layout(
                coloraxis_showscale=False
            )
            rank_col.plotly_chart(
                style_fig(rank_fig, height=490),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

    if not daily.empty:
        daily["DATE"] = pd.to_datetime(daily["DATE"])

        # Anchor the selected window to the latest date available in the
        # warehouse. This keeps synthetic/historical demo data analytically
        # useful even when its measurement year is older than today's date.
        max_available_date = daily["DATE"].max()
        if pd.notna(max_available_date):
            daily = daily[
                daily["DATE"]
                >= max_available_date - pd.Timedelta(days=int(window))
            ].copy()

        for col in ["AVG_AQI", "MAX_AQI", "AVG_PM25"]:
            daily[col] = pd.to_numeric(
                daily[col],
                errors="coerce",
            )

        section("Trend intelligence")

        provinces = sorted(
            daily["CITY_NAME"].dropna().unique()
        )
        default_provinces = (
            daily.groupby("CITY_NAME")["AVG_AQI"]
            .mean()
            .nlargest(min(5, len(provinces)))
            .index.tolist()
        )

        selected = st.multiselect(
            "Compare provinces / city",
            provinces,
            default=default_provinces,
        )

        trend_view = daily[
            daily["CITY_NAME"].isin(selected)
        ]

        if not trend_view.empty:
            trend = px.line(
                trend_view,
                x="DATE",
                y="AVG_AQI",
                color="CITY_NAME",
                title=f"Average AQI — last {window} days",
            )
            st.plotly_chart(
                style_fig(trend, height=430),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        left, right = st.columns(2, gap="large")

        rollup = (
            daily.groupby(
                ["CITY_NAME", "REGION"],
                as_index=False,
            )
            .agg(
                AVG_AQI=("AVG_AQI", "mean"),
                PEAK_AQI=("MAX_AQI", "max"),
                AVG_PM25=("AVG_PM25", "mean"),
            )
            .dropna(subset=["AVG_AQI"])
        )

        if not rollup.empty:
            relation = px.scatter(
                rollup,
                x="AVG_PM25",
                y="AVG_AQI",
                size="PEAK_AQI",
                color="REGION",
                hover_name="CITY_NAME",
                title="PM2.5 relationship with AQI",
                labels={
                    "AVG_PM25": "Average PM2.5",
                    "AVG_AQI": "Average AQI",
                },
            )
            left.plotly_chart(
                style_fig(relation),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        region_rollup = (
            daily.groupby("REGION", as_index=False)
            .agg(
                AVG_AQI=("AVG_AQI", "mean"),
                AVG_PM25=("AVG_PM25", "mean"),
            )
            .sort_values("AVG_AQI")
        )

        if not region_rollup.empty:
            region_fig = px.bar(
                region_rollup,
                x="AVG_AQI",
                y="REGION",
                orientation="h",
                text_auto=".1f",
                title="Analytical-zone comparison",
                hover_data=["AVG_PM25"],
            )
            right.plotly_chart(
                style_fig(region_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )


# ============================================================
# 2) PROVINCE INTELLIGENCE
# ============================================================
elif page == "province":
    header(
        "Province Intelligence",
        "Drill into one Mekong Delta province/city using AQI, particulate matter, weather and intraday patterns.",
    )

    provinces = safe_query(
        """
        SELECT
            c.CITY_SLUG,
            c.CITY_NAME,
            c.REGION
        FROM SILVER.DIM_CITY c
        JOIN CONTROL.CITY_REFERENCE r
          ON r.CITY_SLUG = c.CITY_SLUG
        ORDER BY c.CITY_NAME
        """
    )

    if provinces.empty:
        st.info("Mekong city dimension is not available yet.")
    else:
        label_to_slug = dict(
            zip(
                provinces["CITY_NAME"],
                provinces["CITY_SLUG"],
            )
        )

        c1, c2 = st.columns([2, 1])
        province_name = c1.selectbox(
            "Province / city",
            list(label_to_slug),
        )
        history = c2.selectbox(
            "History",
            [30, 90, 365, 9999],
            format_func=lambda x: (
                "All available"
                if x == 9999
                else f"Last {x} days"
            ),
        )

        slug = label_to_slug[province_name]

        fact = safe_query(
            """
            SELECT
                f.MEASURED_AT,
                f.AQI,
                f.PM25,
                f.PM10,
                f.HUMIDITY,
                f.TEMPERATURE,
                f.POLLUTION_LEVEL,
                f.SOURCE
            FROM SILVER.FACT_AQI f
            JOIN SILVER.DIM_CITY c
              ON c.CITY_KEY = f.CITY_KEY
            WHERE c.CITY_SLUG = %s
            ORDER BY f.MEASURED_AT
            """,
            (slug,),
        )

        hourly = safe_query(
            """
            SELECT HOUR, AVG_AQI
            FROM GOLD.AQI_HOURLY_PATTERN
            WHERE CITY_NAME = %s
            ORDER BY HOUR
            """,
            (province_name,),
        )

        if fact.empty:
            st.info(
                f"No measurements found for {province_name}."
            )
        else:
            fact["MEASURED_AT"] = pd.to_datetime(
                fact["MEASURED_AT"],
                errors="coerce",
            )

            if history != 9999:
                max_available_ts = fact["MEASURED_AT"].max()
                if pd.notna(max_available_ts):
                    fact = fact[
                        fact["MEASURED_AT"]
                        >= max_available_ts - pd.Timedelta(days=int(history))
                    ].copy()

            for col in [
                "AQI",
                "PM25",
                "PM10",
                "HUMIDITY",
                "TEMPERATURE",
            ]:
                fact[col] = pd.to_numeric(
                    fact[col],
                    errors="coerce",
                )

            latest_row = (
                fact.dropna(subset=["MEASURED_AT"])
                .sort_values("MEASURED_AT")
                .tail(1)
            )

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Measurements", f"{len(fact):,}")
            k2.metric("Average AQI", f"{fact['AQI'].mean():.1f}")
            k3.metric("Peak AQI", f"{fact['AQI'].max():.0f}")
            k4.metric(
                "Average PM2.5",
                f"{fact['PM25'].mean():.1f}"
                if fact["PM25"].notna().any()
                else "N/A",
            )
            k5.metric(
                "Latest status",
                (
                    str(latest_row.iloc[0]["POLLUTION_LEVEL"])
                    if not latest_row.empty
                    else "N/A"
                ),
            )

            section("Air-quality timeline")

            daily_city = (
                fact.dropna(subset=["MEASURED_AT"])
                .set_index("MEASURED_AT")
                .resample("D")
                .agg(
                    AQI=("AQI", "mean"),
                    PM25=("PM25", "mean"),
                    PM10=("PM10", "mean"),
                )
                .reset_index()
            )

            long = daily_city.melt(
                id_vars="MEASURED_AT",
                value_vars=["AQI", "PM25", "PM10"],
                var_name="METRIC",
                value_name="VALUE",
            ).dropna(subset=["VALUE"])

            timeline = px.line(
                long,
                x="MEASURED_AT",
                y="VALUE",
                color="METRIC",
                title=f"{province_name} — daily AQI / PM profile",
            )
            st.plotly_chart(
                style_fig(timeline, height=430),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            left, right = st.columns(2, gap="large")

            distribution = (
                fact.groupby("POLLUTION_LEVEL", dropna=False)
                .size()
                .reset_index(name="RECORDS")
            )
            distribution["POLLUTION_LEVEL"] = (
                distribution["POLLUTION_LEVEL"]
                .fillna("Unknown")
            )

            donut = px.pie(
                distribution,
                names="POLLUTION_LEVEL",
                values="RECORDS",
                hole=.56,
                color="POLLUTION_LEVEL",
                color_discrete_map=POLLUTION_COLORS,
                title="Pollution-level distribution",
            )
            left.plotly_chart(
                style_fig(donut),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            weather = fact.dropna(
                subset=["TEMPERATURE", "AQI"]
            )

            if not weather.empty:
                weather_fig = px.scatter(
                    weather,
                    x="TEMPERATURE",
                    y="AQI",
                    size="HUMIDITY",
                    color="AQI",
                    color_continuous_scale="RdYlGn_r",
                    hover_data=[
                        "MEASURED_AT",
                        "PM25",
                        "PM10",
                    ],
                    title="AQI vs temperature / humidity",
                )
                weather_fig.update_layout(
                    coloraxis_showscale=False
                )
                right.plotly_chart(
                    style_fig(weather_fig),
                    use_container_width=True,
                    config=PLOT_CONFIG,
                )

            left2, right2 = st.columns(2, gap="large")

            if not hourly.empty:
                hourly["AVG_AQI"] = pd.to_numeric(
                    hourly["AVG_AQI"],
                    errors="coerce",
                )

                hour_fig = px.area(
                    hourly,
                    x="HOUR",
                    y="AVG_AQI",
                    markers=True,
                    title="Typical AQI by hour",
                )
                left2.plotly_chart(
                    style_fig(hour_fig),
                    use_container_width=True,
                    config=PLOT_CONFIG,
                )

            source = (
                fact.groupby("SOURCE", dropna=False)
                .agg(
                    RECORDS=("AQI", "size"),
                    AVG_AQI=("AQI", "mean"),
                    AVG_PM25=("PM25", "mean"),
                )
                .reset_index()
            )
            source["SOURCE_LABEL"] = source["SOURCE"].replace(
                {
                    "api": "Streaming API",
                    "kaggle": "Historical batch",
                }
            )

            source_fig = px.bar(
                source,
                x="SOURCE_LABEL",
                y="RECORDS",
                text_auto=True,
                hover_data=["AVG_AQI", "AVG_PM25"],
                title="Source coverage",
            )
            right2.plotly_chart(
                style_fig(source_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            with st.expander("Raw measurements"):
                st.dataframe(
                    fact.sort_values(
                        "MEASURED_AT",
                        ascending=False,
                    ),
                    use_container_width=True,
                    hide_index=True,
                )


# ============================================================
# 3) SPATIAL & TEMPORAL
# ============================================================
elif page == "spatial":
    header(
        "Spatial & Temporal Analysis",
        "Compare analytical zones, province-level patterns, hour-of-day behaviour and seasonality across the Mekong Delta.",
    )

    regional = safe_query(
        """
        SELECT g.*
        FROM GOLD.AQI_REGION_SUMMARY g
        WHERE g.REGION IN (
            SELECT DISTINCT REGION
            FROM CONTROL.CITY_REFERENCE
        )
        ORDER BY YEAR, MONTH, REGION
        """
    )

    city_rollup = safe_query(
        """
        SELECT
            c.REGION,
            c.CITY_NAME,
            AVG(f.AQI) AS AVG_AQI,
            MAX(f.AQI) AS MAX_AQI,
            AVG(f.PM25) AS AVG_PM25,
            COUNT(*) AS RECORDS
        FROM SILVER.FACT_AQI f
        JOIN SILVER.DIM_CITY c
          ON c.CITY_KEY = f.CITY_KEY
        JOIN CONTROL.CITY_REFERENCE r
          ON r.CITY_SLUG = c.CITY_SLUG
        GROUP BY c.REGION, c.CITY_NAME
        ORDER BY AVG_AQI DESC
        """
    )

    hourly = safe_query(
        """
        SELECT h.*
        FROM GOLD.AQI_HOURLY_PATTERN h
        WHERE h.CITY_NAME IN (
            SELECT CITY_NAME
            FROM CONTROL.CITY_REFERENCE
        )
        ORDER BY CITY_NAME, HOUR
        """
    )

    seasonal = safe_query(
        """
        SELECT s.*
        FROM GOLD.AQI_SEASONAL_SUMMARY s
        WHERE s.CITY_NAME IN (
            SELECT CITY_NAME
            FROM CONTROL.CITY_REFERENCE
        )
        ORDER BY CITY_NAME, YEAR, SEASON
        """
    )

    monthly = safe_query(
        """
        SELECT
            d.CITY_NAME,
            YEAR(d.DATE) AS YEAR,
            MONTH(d.DATE) AS MONTH,
            AVG(d.AVG_AQI) AS AVG_AQI
        FROM GOLD.AQI_DAILY_SUMMARY d
        WHERE d.CITY_NAME IN (
            SELECT CITY_NAME
            FROM CONTROL.CITY_REFERENCE
        )
        GROUP BY d.CITY_NAME, YEAR(d.DATE), MONTH(d.DATE)
        ORDER BY YEAR, MONTH, CITY_NAME
        """
    )

    if regional.empty:
        st.info("Regional Gold data is not available yet.")
    else:
        regional["AVG_AQI"] = pd.to_numeric(
            regional["AVG_AQI"],
            errors="coerce",
        )
        regional["AVG_PM25"] = pd.to_numeric(
            regional["AVG_PM25"],
            errors="coerce",
        )
        regional["PERIOD"] = (
            regional["YEAR"].astype(str)
            + "-"
            + regional["MONTH"].astype(str).str.zfill(2)
        )

        latest_period = regional["PERIOD"].max()
        latest_regions = regional[
            regional["PERIOD"] == latest_period
        ]

        peak_hour = None
        if not hourly.empty:
            hourly["AVG_AQI"] = pd.to_numeric(
                hourly["AVG_AQI"],
                errors="coerce",
            )
            hour_mean = hourly.groupby(
                "HOUR",
                as_index=False,
            )["AVG_AQI"].mean()

            if not hour_mean.empty:
                peak_hour = int(
                    hour_mean.nlargest(1, "AVG_AQI")
                    .iloc[0]["HOUR"]
                )

        worst_region = (
            latest_regions.nlargest(1, "AVG_AQI")
            if not latest_regions.empty
            else pd.DataFrame()
        )

        k1, k2, k3, k4 = st.columns(4)
        k1.metric(
            "Analytical zones",
            regional["REGION"].nunique(),
        )
        k2.metric("Latest period", latest_period)
        k3.metric(
            "Highest zone avg AQI",
            (
                f"{worst_region.iloc[0]['AVG_AQI']:.1f}"
                if not worst_region.empty
                else "N/A"
            ),
            (
                worst_region.iloc[0]["REGION"]
                if not worst_region.empty
                else None
            ),
            delta_color="off",
        )
        k4.metric(
            "Peak average hour",
            (
                f"{peak_hour:02d}:00"
                if peak_hour is not None
                else "N/A"
            ),
        )

        section("Regional comparison")

        line = px.line(
            regional,
            x="PERIOD",
            y="AVG_AQI",
            color="REGION",
            markers=True,
            title="AQI by analytical zone over time",
        )
        st.plotly_chart(
            style_fig(line, height=420),
            use_container_width=True,
            config=PLOT_CONFIG,
        )

        left, right = st.columns(2, gap="large")

        if not latest_regions.empty:
            bar = px.bar(
                latest_regions.sort_values("AVG_AQI"),
                x="AVG_AQI",
                y="REGION",
                orientation="h",
                color="AVG_AQI",
                color_continuous_scale="RdYlGn_r",
                text_auto=".1f",
                title=f"Latest zone AQI — {latest_period}",
            )
            bar.update_layout(coloraxis_showscale=False)
            left.plotly_chart(
                style_fig(bar),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        if not city_rollup.empty:
            city_rollup["AVG_AQI"] = pd.to_numeric(
                city_rollup["AVG_AQI"],
                errors="coerce",
            )
            city_fig = px.bar(
                city_rollup.sort_values("AVG_AQI"),
                x="AVG_AQI",
                y="CITY_NAME",
                color="REGION",
                orientation="h",
                hover_data=[
                    "MAX_AQI",
                    "AVG_PM25",
                    "RECORDS",
                ],
                title="Province/city average AQI",
            )
            right.plotly_chart(
                style_fig(city_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        section("Temporal patterns")

        if not monthly.empty:
            monthly["PERIOD"] = (
                monthly["YEAR"].astype(str)
                + "-"
                + monthly["MONTH"].astype(str).str.zfill(2)
            )
            monthly["AVG_AQI"] = pd.to_numeric(
                monthly["AVG_AQI"],
                errors="coerce",
            )

            heat = monthly.pivot_table(
                index="CITY_NAME",
                columns="PERIOD",
                values="AVG_AQI",
                aggfunc="mean",
            )

            month_heat = px.imshow(
                heat,
                aspect="auto",
                color_continuous_scale="RdYlGn_r",
                labels={
                    "x": "Month",
                    "y": "Province / city",
                    "color": "Avg AQI",
                },
                title="Province × month AQI heatmap",
            )
            st.plotly_chart(
                style_fig(
                    month_heat,
                    height=max(430, len(heat) * 34),
                ),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        if not hourly.empty:
            hour_heat = hourly.pivot_table(
                index="CITY_NAME",
                columns="HOUR",
                values="AVG_AQI",
                aggfunc="mean",
            )

            fig = px.imshow(
                hour_heat,
                aspect="auto",
                color_continuous_scale="RdYlGn_r",
                labels={
                    "x": "Hour",
                    "y": "Province / city",
                    "color": "Avg AQI",
                },
                title="Province × hour AQI heatmap",
            )
            st.plotly_chart(
                style_fig(
                    fig,
                    height=max(430, len(hour_heat) * 34),
                ),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        if not seasonal.empty:
            seasonal["AVG_AQI"] = pd.to_numeric(
                seasonal["AVG_AQI"],
                errors="coerce",
            )

            season_rollup = (
                seasonal.groupby(
                    "SEASON",
                    as_index=False,
                )["AVG_AQI"]
                .mean()
                .sort_values("AVG_AQI", ascending=False)
            )

            season_fig = px.bar(
                season_rollup,
                x="SEASON",
                y="AVG_AQI",
                text_auto=".1f",
                title="Average AQI by dbt season bucket",
            )
            st.plotly_chart(
                style_fig(season_fig, height=360),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        st.caption(
            "The zone labels in this dashboard are analytical groupings created for the portfolio and are not official administrative regions."
        )


# ============================================================
# 4) PLATFORM HEALTH
# ============================================================
elif page == "health":
    header(
        "Platform Health & Data Quality",
        "Operational view across Kafka ingestion, Snowflake layers, Airflow orchestration, dbt and the data-quality gate.",
    )

    counts = safe_query(
        """
        SELECT
          (SELECT COUNT(*) FROM BRONZE.API_RAW)
            AS BRONZE_API,
          (SELECT COUNT(*) FROM BRONZE.HISTORICAL_CSV)
            AS BRONZE_HISTORICAL,
          (SELECT COUNT(*) FROM SILVER.FACT_AQI)
            AS SILVER_FACT,
          (SELECT COUNT(*) FROM GOLD.AQI_DAILY_SUMMARY)
            AS GOLD_DAILY,
          (SELECT MAX(INGESTED_AT) FROM BRONZE.API_RAW)
            AS LAST_STREAM_INGEST
        """
    )

    runs = safe_query(
        """
        SELECT *
        FROM CONTROL.PIPELINE_RUNS
        ORDER BY STARTED_AT DESC
        LIMIT 100
        """
    )

    tasks = safe_query(
        """
        SELECT *
        FROM CONTROL.PIPELINE_TASK_EVENTS
        ORDER BY EVENT_AT DESC
        LIMIT 500
        """
    )

    dq = safe_query(
        """
        SELECT *
        FROM CONTROL.DQ_RESULTS
        ORDER BY CHECKED_AT DESC
        LIMIT 1000
        """
    )

    source = safe_query(
        """
        SELECT
            f.SOURCE,
            c.CITY_NAME,
            COUNT(*) AS RECORDS,
            MIN(f.MEASURED_AT) AS FIRST_MEASUREMENT,
            MAX(f.MEASURED_AT) AS LAST_MEASUREMENT,
            AVG(f.AQI) AS AVG_AQI,
            AVG(f.PM25) AS AVG_PM25
        FROM SILVER.FACT_AQI f
        JOIN SILVER.DIM_CITY c
          ON c.CITY_KEY = f.CITY_KEY
        JOIN CONTROL.CITY_REFERENCE r
          ON r.CITY_SLUG = c.CITY_SLUG
        GROUP BY f.SOURCE, c.CITY_NAME
        ORDER BY f.SOURCE, c.CITY_NAME
        """
    )

    if not counts.empty:
        row = counts.iloc[0]
        bronze_total = (
            int(row["BRONZE_API"])
            + int(row["BRONZE_HISTORICAL"])
        )
        lag = lag_minutes(row["LAST_STREAM_INGEST"])

        latest_dq_rate = None

        if not dq.empty:
            latest_run = dq.iloc[0]["RUN_ID"]
            latest_dq = dq[
                dq["RUN_ID"] == latest_run
            ].copy()

            if not latest_dq.empty:
                latest_dq["PASSED"] = (
                    latest_dq["PASSED"].astype(bool)
                )
                latest_dq_rate = (
                    latest_dq["PASSED"].mean() * 100
                )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Bronze rows", f"{bronze_total:,}")
        k2.metric(
            "Silver fact rows",
            f"{int(row['SILVER_FACT']):,}",
        )
        k3.metric(
            "Gold daily rows",
            f"{int(row['GOLD_DAILY']):,}",
        )
        k4.metric(
            "Streaming lag",
            f"{lag:.1f} min"
            if lag is not None
            else "N/A",
        )
        k5.metric(
            "Latest DQ pass rate",
            (
                f"{latest_dq_rate:.0f}%"
                if latest_dq_rate is not None
                else "N/A"
            ),
        )

    pipeline_tab, dq_tab, lineage_tab = st.tabs(
        [
            "Pipeline",
            "Data Quality",
            "Sources & Lineage",
        ]
    )

    with pipeline_tab:
        left, right = st.columns(2, gap="large")

        if not counts.empty:
            row = counts.iloc[0]
            layer_df = pd.DataFrame(
                {
                    "LAYER": [
                        "Bronze API",
                        "Bronze Historical",
                        "Silver Fact",
                        "Gold Daily",
                    ],
                    "ROWS": [
                        int(row["BRONZE_API"]),
                        int(row["BRONZE_HISTORICAL"]),
                        int(row["SILVER_FACT"]),
                        int(row["GOLD_DAILY"]),
                    ],
                }
            )

            layer_fig = px.bar(
                layer_df,
                x="LAYER",
                y="ROWS",
                text_auto=True,
                title="Row volume by data layer",
            )
            left.plotly_chart(
                style_fig(layer_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        if not runs.empty:
            status_counts = (
                runs.groupby("STATUS", dropna=False)
                .size()
                .reset_index(name="RUNS")
            )

            run_fig = px.pie(
                status_counts,
                names="STATUS",
                values="RUNS",
                hole=.58,
                title="Airflow pipeline-run status",
            )
            right.plotly_chart(
                style_fig(run_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        if not tasks.empty:
            task_status = (
                tasks.groupby(
                    ["TASK_NAME", "STATUS"],
                    dropna=False,
                )
                .size()
                .reset_index(name="EVENTS")
            )

            task_fig = px.bar(
                task_status,
                x="TASK_NAME",
                y="EVENTS",
                color="STATUS",
                barmode="stack",
                title="Task-event status by stage",
            )
            st.plotly_chart(
                style_fig(task_fig, height=430),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

        with st.expander("Recent pipeline runs"):
            if not runs.empty:
                st.dataframe(
                    runs,
                    use_container_width=True,
                    hide_index=True,
                )

    with dq_tab:
        if dq.empty:
            st.info("No data-quality results found.")
        else:
            latest_run = dq.iloc[0]["RUN_ID"]
            latest = dq[
                dq["RUN_ID"] == latest_run
            ].copy()

            latest["PASSED"] = latest["PASSED"].astype(bool)
            passed = int(latest["PASSED"].sum())
            failed = int((~latest["PASSED"]).sum())
            total = len(latest)
            pass_rate = (
                passed / total * 100
                if total
                else 0
            )

            q1, q2, q3, q4 = st.columns(4)
            q1.metric("Latest DQ run", str(latest_run)[:12])
            q2.metric("Checks", total)
            q3.metric("Passed", passed)
            q4.metric("Pass rate", f"{pass_rate:.0f}%")

            left, right = st.columns(
                [1, 1.4],
                gap="large",
            )

            gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=pass_rate,
                    number={"suffix": "%"},
                    title={"text": "Latest quality score"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": SUCCESS},
                        "steps": [
                            {
                                "range": [0, 80],
                                "color": "rgba(231,90,90,.16)",
                            },
                            {
                                "range": [80, 95],
                                "color": "rgba(244,185,66,.16)",
                            },
                            {
                                "range": [95, 100],
                                "color": "rgba(46,173,115,.16)",
                            },
                        ],
                    },
                )
            )
            left.plotly_chart(
                style_fig(gauge),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            framework = (
                latest.groupby(
                    ["FRAMEWORK", "PASSED"],
                    dropna=False,
                )
                .size()
                .reset_index(name="CHECKS")
            )
            framework["RESULT"] = framework["PASSED"].map(
                {
                    True: "Passed",
                    False: "Failed",
                }
            )

            framework_fig = px.bar(
                framework,
                x="FRAMEWORK",
                y="CHECKS",
                color="RESULT",
                barmode="stack",
                color_discrete_map={
                    "Passed": SUCCESS,
                    "Failed": DANGER,
                },
                title="Checks by validation framework",
            )
            right.plotly_chart(
                style_fig(framework_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            failed_checks = latest[
                ~latest["PASSED"]
            ]

            if failed_checks.empty:
                st.success(
                    "All checks passed in the latest DQ run."
                )
            else:
                st.error(
                    f"{len(failed_checks)} check(s) failed."
                )

                show_cols = [
                    c
                    for c in [
                        "CHECKED_AT",
                        "FRAMEWORK",
                        "CHECK_NAME",
                        "SEVERITY",
                        "PASSED",
                        "DETAIL",
                    ]
                    if c in failed_checks.columns
                ]

                st.dataframe(
                    failed_checks[show_cols],
                    use_container_width=True,
                    hide_index=True,
                )

    with lineage_tab:
        if source.empty:
            st.info("Source-level data is not available yet.")
        else:
            source["SOURCE_LABEL"] = source["SOURCE"].replace(
                {
                    "api": "Streaming API",
                    "kaggle": "Historical batch",
                }
            )

            left, right = st.columns(2, gap="large")

            records = px.bar(
                source,
                x="CITY_NAME",
                y="RECORDS",
                color="SOURCE_LABEL",
                barmode="group",
                title="Records by province/city and source",
            )
            left.plotly_chart(
                style_fig(records),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            aqi = px.bar(
                source,
                x="CITY_NAME",
                y="AVG_AQI",
                color="SOURCE_LABEL",
                barmode="group",
                title="Average AQI by source",
            )
            right.plotly_chart(
                style_fig(aqi),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            coverage = source.copy()
            coverage["FIRST_MEASUREMENT"] = pd.to_datetime(
                coverage["FIRST_MEASUREMENT"],
                errors="coerce",
            )
            coverage["LAST_MEASUREMENT"] = pd.to_datetime(
                coverage["LAST_MEASUREMENT"],
                errors="coerce",
            )
            coverage["DAYS_COVERED"] = (
                coverage["LAST_MEASUREMENT"]
                - coverage["FIRST_MEASUREMENT"]
            ).dt.total_seconds() / 86400

            coverage_fig = px.bar(
                coverage,
                x="CITY_NAME",
                y="DAYS_COVERED",
                color="SOURCE_LABEL",
                barmode="group",
                title="Measurement time-span coverage",
            )
            st.plotly_chart(
                style_fig(coverage_fig),
                use_container_width=True,
                config=PLOT_CONFIG,
            )

            st.caption(
                "Historical and live paths cover different time windows; this comparison is for lineage and coverage diagnostics, not causal comparison."
            )
