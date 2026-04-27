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
    from kedro.framework.session import KedroSession
    from kedro.framework.startup import bootstrap_project
    from pathlib import Path
    warnings.filterwarnings("ignore")
    return KedroSession, Path, bootstrap_project, go, make_subplots, mo, pd, px


@app.cell
def _(KedroSession, Path, bootstrap_project):
    # Bootstrap the project root
    project_path = Path.cwd()
    bootstrap_project(project_path=project_path)

    # Create a session

    session = KedroSession.create()
    context = session.load_context()
    catalog = context.catalog
    return (catalog,)


@app.cell
def _(mo):
    mo.md("""
    # NYC Crash Data — Vision Zero EDA
    **Dataset**: NYC Motor Vehicle Collisions (2012–present)
    **Goal**: Identify when, where, and why fatal and injury crashes occur to inform Vision Zero policy.
    """)
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
    return (df,)


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
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1 · Temporal Patterns — When Do Crashes Happen?
    """)
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
    return


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
    return


@app.cell
def _(df, px):
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
    return


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
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2 · Geospatial Patterns — Where Do Crashes Happen?
    """)
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
    return (borough,)


@app.cell
def _(borough, go, make_subplots):
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
    return


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
    return (fig_map,)


@app.cell
def _(fig_map):
    fig_map
    return


@app.cell
def _(df, mo):
    #Time slider
    year_slider = mo.ui.slider(
        start=int(df["year"].min()),
        stop=int(df["year"].max()),
        step=1,
        value=2014,
        label="Select Year",
    )
    year_slider
    return (year_slider,)


@app.cell
def _(catalog, df, px, year_slider):
    def _():
        # Map of Fatality Rate by Borough Over Time
        import json

        # ── Borough population from census tracts ────────────────────
        county_to_borough = {
            "005": "BRONX",
            "047": "BROOKLYN",
            "061": "MANHATTAN",
            "081": "QUEENS",
            "085": "STATEN ISLAND",
        }

        gdf = catalog.load("nyc_census_geodf").copy()
        gdf["borough"] = gdf["county"].map(county_to_borough)

        # Aggregate geometry and population to borough level
        borough_geo = gdf.dissolve(
            by="borough",
            aggfunc={"population_total": "sum"},
        ).reset_index()

        # Adjust projection
        borough_geo = borough_geo.to_crs(epsg=4326)

        # ── Crash fatalities by borough and year ─────────────────────
        selected_year = year_slider.value

        crash_borough = (
            df[
                (df["year"] == selected_year) &
                (df["borough"].notna()) &
                (df["borough"] != "")
            ]
            .groupby("borough")
            .agg(
                total_killed=("number_of_persons_killed", "sum"),
                total_crashes=("collision_id", "count"),
            )
            .reset_index()
        )

        # ── Join crash data to borough polygons ──────────────────────
        borough_map = borough_geo.merge(
            crash_borough,
            on="borough",
            how="left",
        ).fillna({"total_killed": 0, "total_crashes": 0})
        borough_map_crs = borough_map.crs
        # Fatalities per 100k population
        borough_map["fatality_rate"] = (
            borough_map["total_killed"] / borough_map["population_total"] * 100_000
        ).round(2)

        # ── Plot ─────────────────────────────────────────────────────
        # ── Plot ─────────────────────────────────────────────────────
        # Build GeoJSON with explicit feature IDs matching the dataframe index
        geojson_data = json.loads(borough_map.geometry.to_json())
        for i, feature in enumerate(geojson_data["features"]):
            feature["id"] = i

    # ── Plot ─────────────────────────────────────────────────────
            # Build GeoJSON with explicit feature IDs matching the dataframe index
            geojson_data = json.loads(borough_map.geometry.to_json())
            for i, feature in enumerate(geojson_data["features"]):
                feature["id"] = i

            fig_borough_map = px.choropleth_mapbox(
                borough_map.reset_index(),
                geojson=geojson_data,
                locations="index",              # ← use the reset index column
                color="fatality_rate",
                featureidkey="id",              # ← tell Plotly where to find the id in GeoJSON
                hover_name="borough",
                hover_data={
                    "fatality_rate": ":.2f",
                    "total_killed": True,
                    "total_crashes": True,
                    "population_total": True,
                    "index": False,             # ← hide index from hover
                },
                color_continuous_scale="Reds",
                mapbox_style="carto-positron",
                zoom=9,
                center={"lat": 40.7128, "lon": -74.0060},
                opacity=0.7,
                title=f"Fatality Rate per 100k Population by Borough — {selected_year}",
                height=600,
                labels={
                    "fatality_rate": "Fatalities per 100k",
                    "total_killed": "Total Killed",
                    "total_crashes": "Total Crashes",
                    "population_total": "Population",
                },
            )
        return fig_borough_map



    _()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The most dangerous streets
    """)
    return


@app.cell
def _(df, pd):
    import re

    def normalize_street_name(name: str) -> str:
        if pd.isna(name):
            return None
        name = name.upper().strip()
        # Collapse multiple spaces
        name = re.sub(r'\s+', ' ', name)
        # Standardize common abbreviations
        replacements = {
            r'\bST\b': 'STREET',
            r'\bAVE\b': 'AVENUE',
            r'\bBLVD\b': 'BOULEVARD',
            r'\bRD\b': 'ROAD',
            r'\bDR\b': 'DRIVE',
            r'\bPL\b': 'PLACE',
            r'\bPKWY\b': 'PARKWAY',
            r'\bEXPY\b': 'EXPRESSWAY',
            r'\bHWY\b': 'HIGHWAY',
            r'\bBDWAY\b': 'BROADWAY',
        }
        for pattern, replacement in replacements.items():
            name = re.sub(pattern, replacement, name)
        return name

    df["street_normalized"] = df["on_street_name"].apply(normalize_street_name)
    return (normalize_street_name,)


@app.cell
def _(df, px):
    # The top dangerous streets for Vurnelable Road Users
    fatal_streets = (
        df[df["any_killed"]]
        .groupby('street_normalized')['number_of_persons_killed']
        .sum()
        .sort_values(ascending = False)
        .head(15)
        .reset_index()
    )

    fig_street_danger = px.bar(
        fatal_streets,
        x="number_of_persons_killed",   
        y="street_normalized",           
        orientation="h",
        title="Top 15 Deadliest Streets — Vulnerable Road Users",
        labels={
            "number_of_persons_killed": "Total Killed",
            "street_normalized": "",     
        },
        color="number_of_persons_killed",
        color_continuous_scale="Reds",
    )
    fig_street_danger.update_layout(
        height=500,
        yaxis={"categoryorder": "total ascending"},
        coloraxis_showscale=False,
    )
    fig_street_danger
    return


@app.cell
def _(df, mo):
    borough_options = mo.ui.dropdown(
        options=["ALL"] + sorted(df["borough"].dropna().unique().tolist()),
        value="ALL",
        label="Select Borough",
    )
    borough_options
    return (borough_options,)


@app.cell
def _(borough_options, df, px):
    def _():
        selected = borough_options.value

        if selected == "ALL":
            filtered = df[df["any_killed"]]
        else:
            filtered = df[df["any_killed"] & (df["borough"] == selected)]

        fatal_streets = (
            filtered
            .groupby("street_normalized")["number_of_persons_killed"]
            .sum()
            .sort_values(ascending=False)
            .head(15)
            .reset_index()
            .sort_values("number_of_persons_killed", ascending=True)
        )

        fig_street_danger = px.bar(
            fatal_streets,
            x="number_of_persons_killed",
            y="street_normalized",
            orientation="h",
            title=f"Top 15 Deadliest Streets — {selected}",
            labels={
                "number_of_persons_killed": "Total Killed",
                "street_normalized": "",
            },
            color="number_of_persons_killed",
            color_continuous_scale="Reds",
        )
        fig_street_danger.update_layout(
            height=500,
            yaxis={"categoryorder": "total ascending"},
            coloraxis_showscale=False,
        )
        return fig_street_danger


    _()
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3 · Severity Analysis — What Factors Predict Fatalities?
    """)
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
    return


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
    return


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
    return


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


@app.cell
def _(pd):
    HIGHWAYS = [
        "BELT PARKWAY", "GRAND CENTRAL PARKWAY", "MAJOR DEEGAN EXPRESSWAY",
        "BRUCKNER BOULEVARD", "HENRY HUDSON PARKWAY", "CROSS ISLAND PARKWAY",
        "LONG ISLAND EXPRESSWAY", "CROSS BRONX EXPRESSWAY",
        "BROOKLYN QUEENS EXPRESSWAY", "BRUCKNER EXPRESSWAY", "FDR DRIVE",
        "STATEN ISLAND EXPRESSWAY", "GOWANUS EXPRESSWAY", "HARLEM RIVER DRIVE",
        "HUTCHINSON RIVER PARKWAY", "SHERIDAN EXPRESSWAY", "VAN WYCK EXPRESSWAY",
        "BELT PARKWAY SERVICE ROAD",
    ]

    def classify_street_type(street: str) -> str:
        if pd.isna(street):
            return "Unknown"
        if street in HIGHWAYS:
            return "Highway"
        if any(x in street for x in ["EXPRESSWAY", "PARKWAY", "FREEWAY"]):
            return "Highway"
        return "Surface Street"

    return (classify_street_type,)


@app.cell
def _(df, normalize_street_name, pd):
    df["street_normalized"] = df["on_street_name"].apply(normalize_street_name)

    def build_intersection_label(row) -> str | None:
        if pd.isna(row["street_normalized"]) or pd.isna(row["cross_street_normalized"]):
            return None
        return " & ".join(sorted([
            str(row["street_normalized"]),
            str(row["cross_street_normalized"]),
        ]))


    def dominant_factor(series: pd.Series) -> str:
        counts = series[series != "Unspecified"].value_counts()
        return counts.index[0] if not counts.empty else "Unspecified"


    def dominant_road_user(row) -> str:
        users = {
            "Pedestrian": row["ped_killed"],
            "Cyclist":    row["cyc_killed"],
            "Motorist":   row["mot_killed"],
        }
        return max(users, key=users.get)


    def get_policy_recommendation(factor: str, road_user: str) -> str:
        recommendations = {
            "Driver Inattention/Distraction": "Speed cameras, signal retiming, distracted driving enforcement",
            "Failure to Yield Right-of-Way": "Leading pedestrian intervals, exclusive pedestrian phases",
            "Speeding": "Speed bumps, road diet, automated speed enforcement",
            "Alcohol Involvement": "Late night enforcement, DUI checkpoints",
            "Following Too Closely": "Speed reduction, congestion management",
            "Traffic Control Disregarded": "Signal upgrades, red light cameras",
            "Unsafe Speed": "Speed cameras, road diet",
        }
        base = recommendations.get(factor, "Review traffic control and enforcement")
        if road_user == "Pedestrian":
            return base + " + crosswalk hardening, pedestrian refuge islands"
        if road_user == "Cyclist":
            return base + " + protected bike lanes, intersection hardening"
        return base

    return (
        build_intersection_label,
        dominant_factor,
        dominant_road_user,
        get_policy_recommendation,
    )


@app.cell
def _(
    build_intersection_label,
    classify_street_type,
    df,
    normalize_street_name,
    pd,
):
    def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["street_normalized"] = (
            df["on_street_name"]
            .apply(normalize_street_name)
            .fillna(df["off_street_name"].apply(normalize_street_name))
        )
        df["off_street_normalized"] = df["off_street_name"].apply(normalize_street_name)
        df["cross_street_normalized"] = df["cross_street_name"].apply(normalize_street_name)
        df["intersection"] = df.apply(build_intersection_label, axis=1)
        df["street_type"] = df["street_normalized"].apply(classify_street_type)
        return df

    df_enriched = enrich_dataframe(df)
    return (df_enriched,)


@app.cell
def _(dominant_factor, dominant_road_user, get_policy_recommendation, pd):
    def compute_street_stats(
        df: pd.DataFrame,
        borough: str = "ALL",
        street_type: str = "ALL",
        top_n: int = 20,
    ) -> pd.DataFrame:

        # ── All crashes per street for correct denominator ────────
        all_crashes = df[df["street_normalized"].notna()]
        if borough != "ALL":
            all_crashes = all_crashes[all_crashes["borough"] == borough]
        if street_type != "ALL":
            all_crashes = all_crashes[all_crashes["street_type"] == street_type]

        group_cols = ["street_normalized"] if borough == "ALL" else ["street_normalized", "borough"]

        all_crash_counts = (
            all_crashes
            .groupby(group_cols)
            .agg(total_crashes=("collision_id", "count"))
            .reset_index()
        )

        # ── Fatal crashes only for killed counts ─────────────────
        fatal = df[df["any_killed"] & df["street_normalized"].notna()]
        if borough != "ALL":
            fatal = fatal[fatal["borough"] == borough]
        if street_type != "ALL":
            fatal = fatal[fatal["street_type"] == street_type]

        stats = (
            fatal
            .groupby(group_cols)
            .agg(
                total_killed=("number_of_persons_killed", "sum"),
                ped_killed=("number_of_pedestrians_killed", "sum"),
                cyc_killed=("number_of_cyclist_killed", "sum"),
                mot_killed=("number_of_motorist_killed", "sum"),
                top_factor=("contributing_factor_vehicle_1", dominant_factor),
                years_active=("year", lambda x: f"{int(x.min())}–{int(x.max())}"),
                street_type=("street_type", "first"),
            )
            .reset_index()
        )

        # ── Join correct denominator ──────────────────────────────
        stats = stats.merge(all_crash_counts, on=group_cols, how="left")

        if borough == "ALL":
            stats["borough"] = "ALL"

        # Fatality rate = killed per 1,000 total crashes on that street
        stats["fatality_rate"] = (
            stats["total_killed"] / stats["total_crashes"] * 1000
        ).round(2)

        stats["dominant_road_user"] = stats.apply(dominant_road_user, axis=1)

        stats["policy_recommendation"] = stats.apply(
            lambda r: get_policy_recommendation(r["top_factor"], r["dominant_road_user"]),
            axis=1,
        )

        return (
            stats
            .sort_values("total_killed", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )

    return (compute_street_stats,)


@app.cell
def _(go, pd, px):
    def plot_street_chart(stats: pd.DataFrame, borough: str) -> go.Figure:
        fig = px.bar(
            stats.sort_values("total_killed", ascending=True),
            x="total_killed",
            y="street_normalized",
            orientation="h",
            color="top_factor",
            hover_data={
                "borough": True,
                "total_crashes": True,
                "fatality_rate": True,
                "dominant_road_user": True,
                "years_active": True,
                "top_factor": True,
                "total_killed": True,
                "policy_recommendation": True,
                "street_normalized": False,
            },
            title=f"Top 20 Deadliest Streets — {borough}",
            labels={
                "total_killed": "Total Killed",
                "street_normalized": "",
                "top_factor": "Top Factor",
                "fatality_rate": "Fatality Rate (%)",
                "dominant_road_user": "Road User Most Affected",
                "total_crashes": "Total Crashes",
                "years_active": "Active Since",
                "policy_recommendation": "Recommendation",
            },
            height=650,
            )
        fig.update_layout(
            title=dict(
                    text=f"Top 20 Deadliest Streets — {borough}",
                    x=0,
                    y=0.98,
                    xanchor="left",
                ),
                yaxis={"categoryorder": "total ascending"},
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.12,        # ← move legend below the chart
                    xanchor="left",
                    x=0,
                    title="Top Contributing Factor",
                ),
                margin=dict(l=20, r=20, t=60, b=120),  # ← increase bottom margin for legend
                height=700,                              # ← slightly taller to compensate
            )
        return fig

    return (plot_street_chart,)


@app.cell
def _(df_enriched, mo):
    borough_filter = mo.ui.dropdown(
        options=["ALL"] + sorted(df_enriched["borough"].dropna().unique().tolist()),
        value="ALL",
        label="Borough",
    )

    street_type_filter = mo.ui.dropdown(
        options=["ALL", "Highway", "Surface Street"],
        value="ALL",
        label="Street Type",
    )

    mo.hstack([borough_filter, street_type_filter])
    return borough_filter, street_type_filter


@app.cell
def _(
    borough_filter,
    compute_street_stats,
    df_enriched,
    mo,
    plot_street_chart,
    street_type_filter,
):
    stats = compute_street_stats(
        df_enriched,
        borough=borough_filter.value,
        street_type=street_type_filter.value,
    )
    fig = plot_street_chart(stats, borough=borough_filter.value)

    agency_note = {
        "Highway": "🏛️ **Responsible Agency**: NYS DOT / MTA — Speed enforcement, highway redesign, managed speed zones",
        "Surface Street": "🏙️ **Responsible Agency**: NYC DOT / NYPD Vision Zero — Signal timing, pedestrian intervals, local enforcement",
        "ALL": "ℹ️ Select a street type to see agency-specific policy recommendations",
    }

    mo.vstack([
        fig,
        mo.md(agency_note[street_type_filter.value]),
        mo.md(f"""
    ### Top 10 Policy Priorities — {street_type_filter.value} | {borough_filter.value}

    | Rank | Street | Killed | Fatality Rate (per 1k crashes) | Factor | Road User | Recommendation |
    |------|--------|--------|--------------------------------|--------|-----------|----------------|
    {"".join(
        f"| {i+1} | {r['street_normalized']} | {int(r['total_killed'])} | {r['fatality_rate']}% | {r['top_factor']} | {r['dominant_road_user']} | {r['policy_recommendation']} |{chr(10)}"
        for i, r in stats.head(10).iterrows()
    )}
        """),
    ])
    return


if __name__ == "__main__":
    app.run()
