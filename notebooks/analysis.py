import marimo

__generated_with = "0.23.2"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import warnings
    warnings.filterwarnings("ignore")
    return go, make_subplots, mo, pd, px, warnings


@app.cell
def _(mo):
    mo.md(
        """
        # NYC Crash Data — Vision Zero EDA
        **Dataset**: NYC Motor Vehicle Collisions (2014–present)  
        **Goal**: Identify when, where, and why fatal and injury crashes occur to inform Vision Zero policy.
        """
    )
    return


@app.cell
def _(pd):
    df = pd.read_parquet("data/02_primary/nyc_crashes.parquet")

    # Parse dates and extract time features
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
    df["crash_time"] = pd.to_datetime(df["crash_time"], format="%H:%M", errors="coerce")
    df["hour"] = df["crash_time"].dt.hour
    df["day_of_week"] = df["crash_date"].dt.day_name()
    df["month"] = df["crash_date"].dt.month
    df["year"] = df["crash_date"].dt.year
    df["month_name"] = df["crash_date"].dt.strftime("%b")

    # Derived severity columns
    df["any_injury"] = (df["number_of_persons_injured"] > 0)
    df["any_killed"] = (df["number_of_persons_killed"] > 0)
    df["pedestrian_involved"] = (
        (df["number_of_pedestrians_injured"] > 0) |
        (df["number_of_pedestrians_killed"] > 0)
    )
    df["cyclist_involved"] = (
        (df["number_of_cyclist_injured"] > 0) |
        (df["number_of_cyclist_killed"] > 0)
    )

    print(f"Loaded {len(df):,} records from {df['crash_date'].min().date()} to {df['crash_date'].max().date()}")
    df.head(3)
    return df


@app.cell
def _(df, mo):
    # Summary stats
    total = len(df)
    total_killed = int(df["number_of_persons_killed"].sum())
    total_injured = int(df["number_of_persons_injured"].sum())
    pct_injury = df["any_injury"].mean() * 100
    ped_killed = int(df["number_of_pedestrians_killed"].sum())
    cyc_killed = int(df["number_of_cyclist_killed"].sum())
    mot_killed = int(df["number_of_motorist_killed"].sum())

    mo.md(f"""
    ## Dataset Overview

    | Metric | Value |
    |--------|-------|
    | Total crashes | {total:,} |
    | Total people killed | {total_killed:,} |
    | Total people injured | {total_injured:,} |
    | Crashes with injury | {pct_injury:.1f}% |
    | Pedestrians killed | {ped_killed:,} |
    | Cyclists killed | {cyc_killed:,} |
    | Motorists killed | {mot_killed:,} |
    """)
    return (
        cyc_killed,
        mot_killed,
        ped_killed,
        pct_injury,
        total,
        total_injured,
        total_killed,
    )


# ─────────────────────────────────────────────────────────────
# SECTION 1: TEMPORAL PATTERNS
# ─────────────────────────────────────────────────────────────

@app.cell
def _(mo):
    mo.md("## 1 · Temporal Patterns — When Do Crashes Happen?")
    return


@app.cell
def _(df, px):
    # Crashes and fatalities by hour
    hourly = df.groupby("hour").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
        injured=("number_of_persons_injured", "sum"),
    ).reset_index()

    fig_hour = px.bar(
        hourly, x="hour", y="crashes",
        title="Crashes by Hour of Day",
        labels={"hour": "Hour of Day", "crashes": "Number of Crashes"},
        color="crashes",
        color_continuous_scale="Reds",
    )
    fig_hour.update_layout(coloraxis_showscale=False, height=350)
    fig_hour
    return fig_hour, hourly


@app.cell
def _(df, px):
    # Fatality rate by hour (Vision Zero focus)
    hourly_fatal = df.groupby("hour").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
    ).reset_index()
    hourly_fatal["fatality_rate"] = hourly_fatal["killed"] / hourly_fatal["crashes"] * 1000

    fig_fatal_hour = px.line(
        hourly_fatal, x="hour", y="fatality_rate",
        title="Fatalities per 1,000 Crashes by Hour (Vision Zero Lens)",
        labels={"hour": "Hour of Day", "fatality_rate": "Fatalities per 1,000 Crashes"},
        markers=True,
    )
    fig_fatal_hour.update_traces(line_color="#d62728", marker_color="#d62728")
    fig_fatal_hour.update_layout(height=350)
    fig_fatal_hour
    return fig_fatal_hour, hourly_fatal


@app.cell
def _(df, pd, px):
    # Day of week pattern
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow = df.groupby("day_of_week").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
    ).reindex(dow_order).reset_index()
    dow["fatality_rate"] = dow["killed"] / dow["crashes"] * 1000

    fig_dow = px.bar(
        dow, x="day_of_week", y="crashes",
        title="Crashes by Day of Week",
        labels={"day_of_week": "", "crashes": "Number of Crashes"},
        color="fatality_rate",
        color_continuous_scale="Oranges",
        color_continuous_midpoint=dow["fatality_rate"].median(),
    )
    fig_dow.update_layout(height=350, coloraxis_colorbar_title="Fatalities<br>per 1k crashes")
    fig_dow
    return dow, dow_order, fig_dow


@app.cell
def _(df, px):
    # Annual trend — Vision Zero launched 2014
    annual = df.groupby("year").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
        injured=("number_of_persons_injured", "sum"),
    ).reset_index()
    annual = annual[annual["year"] >= 2014]

    fig_annual = px.line(
        annual, x="year", y=["killed", "injured"],
        title="Annual Fatalities and Injuries Since Vision Zero Launch (2014)",
        labels={"year": "Year", "value": "Count", "variable": ""},
        markers=True,
        color_discrete_map={"killed": "#d62728", "injured": "#ff7f0e"},
    )
    fig_annual.update_layout(height=380)
    fig_annual
    return annual, fig_annual


# ─────────────────────────────────────────────────────────────
# SECTION 2: GEOSPATIAL PATTERNS
# ─────────────────────────────────────────────────────────────

@app.cell
def _(mo):
    mo.md("## 2 · Geospatial Patterns — Where Do Crashes Happen?")
    return


@app.cell
def _(df, px):
    # Borough breakdown
    borough_df = df[df["borough"].notna() & (df["borough"] != "")].copy()
    borough = borough_df.groupby("borough").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
        injured=("number_of_persons_injured", "sum"),
        ped_killed=("number_of_pedestrians_killed", "sum"),
        cyc_killed=("number_of_cyclist_killed", "sum"),
    ).reset_index().sort_values("crashes", ascending=False)
    borough["fatality_rate"] = borough["killed"] / borough["crashes"] * 1000

    fig_borough = px.bar(
        borough, x="borough", y="crashes",
        title="Total Crashes by Borough",
        color="fatality_rate",
        color_continuous_scale="Reds",
        labels={"borough": "Borough", "crashes": "Crashes", "fatality_rate": "Fatalities per 1k"},
    )
    fig_borough.update_layout(height=380, coloraxis_colorbar_title="Fatalities<br>per 1k crashes")
    fig_borough
    return borough, borough_df, fig_borough


@app.cell
def _(borough, make_subplots, go):
    # Vulnerable road users by borough
    fig_vru = make_subplots(rows=1, cols=2, subplot_titles=["Pedestrians Killed", "Cyclists Killed"])
    fig_vru.add_trace(
        go.Bar(x=borough["borough"], y=borough["ped_killed"], marker_color="#d62728", name="Pedestrians"),
        row=1, col=1
    )
    fig_vru.add_trace(
        go.Bar(x=borough["borough"], y=borough["cyc_killed"], marker_color="#1f77b4", name="Cyclists"),
        row=1, col=2
    )
    fig_vru.update_layout(
        title="Vulnerable Road User Fatalities by Borough",
        height=380,
        showlegend=False,
    )
    fig_vru
    return fig_vru,


@app.cell
def _(df, px):
    # Scatter map of fatal crashes
    fatal_map = df[df["any_killed"] & df["latitude"].notna() & df["longitude"].notna()].copy()
    fatal_map = fatal_map[
        (fatal_map["latitude"].between(40.4, 40.95)) &
        (fatal_map["longitude"].between(-74.3, -73.7))
    ]

    fig_map = px.scatter_mapbox(
        fatal_map,
        lat="latitude",
        lon="longitude",
        color="number_of_persons_killed",
        size="number_of_persons_killed",
        hover_data=["crash_date", "borough", "on_street_name", "contributing_factor_vehicle_1"],
        color_continuous_scale="Reds",
        zoom=10,
        mapbox_style="carto-positron",
        title=f"Fatal Crash Locations ({len(fatal_map):,} crashes)",
        opacity=0.6,
        height=550,
    )
    fig_map.update_layout(coloraxis_colorbar_title="People<br>Killed")
    fig_map
    return fatal_map, fig_map


# ─────────────────────────────────────────────────────────────
# SECTION 3: SEVERITY ANALYSIS
# ─────────────────────────────────────────────────────────────

@app.cell
def _(mo):
    mo.md("## 3 · Severity Analysis — What Factors Predict Fatalities?")
    return


@app.cell
def _(df, px):
    # Top contributing factors for fatal crashes vs all crashes
    fatal_factors = (
        df[df["any_killed"]]["contributing_factor_vehicle_1"]
        .value_counts()
        .head(15)
        .reset_index()
    )
    fatal_factors.columns = ["factor", "fatal_crashes"]

    all_factors = (
        df["contributing_factor_vehicle_1"]
        .value_counts()
        .head(15)
        .reset_index()
    )
    all_factors.columns = ["factor", "all_crashes"]

    fig_factors = px.bar(
        fatal_factors[fatal_factors["factor"] != "Unspecified"],
        x="fatal_crashes", y="factor",
        orientation="h",
        title="Top Contributing Factors in Fatal Crashes",
        labels={"fatal_crashes": "Fatal Crashes", "factor": ""},
        color="fatal_crashes",
        color_continuous_scale="Reds",
    )
    fig_factors.update_layout(height=420, yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
    fig_factors
    return all_factors, fatal_factors, fig_factors


@app.cell
def _(df, px):
    # Pedestrian fatalities by hour
    ped_hourly = df.groupby("hour").agg(
        ped_killed=("number_of_pedestrians_killed", "sum"),
        cyc_killed=("number_of_cyclist_killed", "sum"),
        mot_killed=("number_of_motorist_killed", "sum"),
    ).reset_index()

    fig_vru_hour = px.line(
        ped_hourly, x="hour",
        y=["ped_killed", "cyc_killed", "mot_killed"],
        title="Fatalities by Road User Type and Hour of Day",
        labels={"hour": "Hour of Day", "value": "Total Killed", "variable": ""},
        markers=True,
        color_discrete_map={
            "ped_killed": "#d62728",
            "cyc_killed": "#1f77b4",
            "mot_killed": "#ff7f0e",
        },
    )
    fig_vru_hour.update_layout(height=380)
    fig_vru_hour
    return fig_vru_hour, ped_hourly


@app.cell
def _(df, pd, px):
    # Vision Zero progress: fatality rate per 100k crashes by year
    vz = df[df["year"] >= 2014].groupby("year").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
        ped_killed=("number_of_pedestrians_killed", "sum"),
        cyc_killed=("number_of_cyclist_killed", "sum"),
    ).reset_index()
    vz["fatality_rate"] = vz["killed"] / vz["crashes"] * 100000
    vz["ped_rate"] = vz["ped_killed"] / vz["crashes"] * 100000
    vz["cyc_rate"] = vz["cyc_killed"] / vz["crashes"] * 100000

    # Exclude incomplete current year
    current_year = pd.Timestamp.now().year
    vz = vz[vz["year"] < current_year]

    fig_vz = px.line(
        vz, x="year",
        y=["fatality_rate", "ped_rate", "cyc_rate"],
        title="Vision Zero Progress: Fatality Rate per 100,000 Crashes by Year",
        labels={"year": "Year", "value": "Rate per 100k Crashes", "variable": ""},
        markers=True,
        color_discrete_map={
            "fatality_rate": "#2ca02c",
            "ped_rate": "#d62728",
            "cyc_rate": "#1f77b4",
        },
    )
    fig_vz.update_layout(height=400)
    fig_vz
    return current_year, fig_vz, vz


@app.cell
def _(mo):
    mo.md("""
    ## Key Findings for Policy

    Use the charts above to fill in findings. Suggested policy-relevant questions to answer:

    - **Temporal**: Which hours and days concentrate the most fatalities? (late night / early morning weekend pattern is common)
    - **Geospatial**: Which boroughs have the highest fatality *rates* (not just counts)? Which streets recur most in fatal crashes?
    - **Severity**: Is driver inattention / distraction the dominant factor in fatal crashes? How have pedestrian and cyclist fatality rates trended since 2014?
    - **Vision Zero**: Is the fatality rate per crash improving over time, or are raw numbers declining only because total crashes declined (e.g. COVID)?
    """)
    return


if __name__ == "__main__":
    app.run()
