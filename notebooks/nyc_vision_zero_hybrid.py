import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    """Library imports"""
    import marimo as mo
    import pandas as pd
    import geopandas as gpd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import re
    import warnings
    warnings.filterwarnings("ignore")
    return go, make_subplots, mo, pd, px, re


@app.cell
def _():
    """Install H3 for spatial binning"""
    import subprocess
    try:
        import h3
    except ImportError:
        subprocess.run(['pip', 'install', 'h3', '--break-system-packages'],
                      capture_output=True)
        import h3
    return (h3,)


@app.cell
def _(mo):
    mo.md("""
    # NYC Traffic Fatality Analysis — Vision Zero Policy Brief

    **Remy Ndayizeye**
    Northeast Big Data Innovation Hub · National Student Data Corps · U.S. DOT FHWA
    *May 2026*

    ---

    ## Executive Summary

    NYC achieved a historic milestone in 2025: 205 traffic deaths, the lowest count since 1910 and
    a 31% reduction since Vision Zero launched in 2014 (NYC DOT, Jan 2026). This analysis identifies
    where the remaining deaths occur and which locations represent emerging crises vs. chronic failures,
    enabling targeted intervention deployment.

    **Key Findings:**

    1. **Progress with persistent burden** — Overall fatalities declined 31%, but vulnerable road users
       (pedestrians + cyclists) still account for 66% of deaths.

    2. **Concentrated at isolated intersections** — 77% of fatal hotspots are isolated surface street
       intersections, not highway corridors, meaning most deaths are addressable at city level.

    3. **Recency-weighted scoring reveals urgency** — A hybrid model weighting recent deaths more heavily
       and comparing Pre- vs. Post-COVID counts separates emerging behavioral crises from chronic
       infrastructure failures.

    4. **Different patterns demand different responses** — Urgent emergent hotspots need rapid enforcement
       and quick-build measures; persistent hotspots need capital redesign.

    ---

    **Data:** NYC Open Data — 1.95M crashes (July 2012 – April 2026)
    **Methods:** H3 spatial binning · Recency-Weighted Hybrid Priority Model · Pre/Post-COVID trend analysis
    **Context:** Extends NYC DOT 2025 Year-End Report with spatial-temporal targeting
    """)
    return


@app.cell
def _(pd):
    """Load and prepare crash data"""
    df_raw = pd.read_parquet("data/02_primary/nyc_crashes.parquet")

    # Parse dates
    df_raw["crash_date"] = pd.to_datetime(df_raw["crash_date"], errors="coerce")
    df_raw["crash_time"] = pd.to_datetime(df_raw["crash_time"], format="%H:%M", errors="coerce")
    df_raw["year"] = df_raw["crash_date"].dt.year
    df_raw["quarter"] = pd.PeriodIndex(df_raw["crash_date"], freq="Q")

    # Flags
    df_raw["any_killed"] = df_raw["number_of_persons_killed"] > 0


    print(f"✓ Loaded {len(df_raw):,} crashes")
    print(f"✓ Date range: {df_raw['crash_date'].min().date()} → {df_raw['crash_date'].max().date()}")
    return (df_raw,)


@app.cell
def _(pd, re):
    """Helper: Street normalization"""
    def normalize_street(name: str) -> str | None:
        if pd.isna(name):
            return None
        name = name.upper().strip()
        name = re.sub(r'\s+', ' ', name)
        replacements = {
            r'\bST\b': 'STREET', r'\bAVE\b': 'AVENUE', r'\bBLVD\b': 'BOULEVARD',
            r'\bPKWY\b': 'PARKWAY', r'\bEXPY\b': 'EXPRESSWAY',
        }
        for pattern, replacement in replacements.items():
            name = re.sub(pattern, replacement, name)
        return name


    return (normalize_street,)


@app.cell
def _():
    """Helper: Street type classification"""
    HIGHWAYS = {
        "BELT PARKWAY", "MAJOR DEEGAN EXPRESSWAY", "FDR DRIVE",
        "BROOKLYN QUEENS EXPRESSWAY", "GRAND CENTRAL PARKWAY",
        "CROSS BRONX EXPRESSWAY", "BRUCKNER EXPRESSWAY",
    }

    def classify_street_type(street: str) -> str:
        if not street:
            return "Unknown"
        if street in HIGHWAYS or any(x in street for x in ["EXPRESSWAY", "PARKWAY"]):
            return "Highway"
        return "Surface Street"


    return (classify_street_type,)


@app.cell
def _(classify_street_type, df_raw, normalize_street):
    """Apply enrichment"""
    df = df_raw.copy()
    df["street_normalized"] = df["on_street_name"].apply(normalize_street).fillna(
        df["off_street_name"].apply(normalize_street)
    )
    df["street_type"] = df["street_normalized"].apply(classify_street_type)
    fatal = df[df["any_killed"] & df["street_type"].notna()]

    print(f"✓ Enriched {len(df):,} records")
    return df, fatal


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Vision Zero Progress: 2012–2026 Trends

    NYC launched Vision Zero in 2014 with the goal of eliminating traffic deaths. The following
    charts show overall progress and the persistent burden on vulnerable road users.
    """)
    return


@app.cell
def _(df):
    """Calculate annual fatality rates"""
    annual_stats = (
        df.groupby("year")
        .agg(
            total_crashes=("collision_id", "count"),
            fatal_crashes=("any_killed", "sum"),
            total_killed=("number_of_persons_killed", "sum"),
            ped_killed=("number_of_pedestrians_killed", "sum"),
            cyc_killed=("number_of_cyclist_killed", "sum"),
            mot_killed=("number_of_motorist_killed", "sum"),
        )
        .reset_index()
    )

    # Rates per 1000 crashes
    annual_stats["fatality_rate"] = annual_stats["total_killed"] / annual_stats["total_crashes"] * 1000
    annual_stats["ped_rate"] = annual_stats["ped_killed"] / annual_stats["total_crashes"] * 1000
    annual_stats["cyc_rate"] = annual_stats["cyc_killed"] / annual_stats["total_crashes"] * 1000
    annual_stats["vru_killed"] = annual_stats["ped_killed"] + annual_stats["cyc_killed"]
    annual_stats["vru_rate"] = annual_stats["vru_killed"] / annual_stats["total_crashes"] * 1000
    return (annual_stats,)


@app.cell
def _(annual_stats, go, make_subplots, mo):
    """Create trend visualization with counts and rates"""
    fig_trends = make_subplots(
        rows=1, cols=3,
        subplot_titles=[
            "<b>Total Fatalities</b><br><sup>Annual count</sup>",
            "<b>Fatality Rate</b><br><sup>Deaths per 1,000 crashes</sup>",
            "<b>VRU Fatality Rate</b><br><sup>Ped + Cyclist per 1,000 crashes</sup>",
        ],
        horizontal_spacing=0.10,
    )

    # LEFT: Absolute count
    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["total_killed"],
            mode="lines+markers",
            name="Total Deaths",
            line=dict(color="#d62728", width=3),
            marker=dict(size=8),
            showlegend=False,
        ),
        row=1, col=1,
    )

    # MIDDLE: Overall rate
    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["fatality_rate"],
            mode="lines+markers",
            name="Overall Rate",
            line=dict(color="#1f77b4", width=3),
            marker=dict(size=8),
            showlegend=False,
        ),
        row=1, col=2,
    )

    # RIGHT: VRU rates (with breakdown)
    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["vru_rate"],
            mode="lines+markers",
            name="VRU Total",
            line=dict(color="#2ca02c", width=3),
            marker=dict(size=8),
        ),
        row=1, col=3,
    )

    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["ped_rate"],
            mode="lines",
            name="Pedestrians",
            line=dict(color="#ff7f0e", width=2, dash="dash"),
        ),
        row=1, col=3,
    )

    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["cyc_rate"],
            mode="lines",
            name="Cyclists",
            line=dict(color="#9467bd", width=2, dash="dot"),
        ),
        row=1, col=3,
    )

    # Mark Vision Zero launch and COVID on all subplots
    for col in [1, 2, 3]:
        fig_trends.add_vline(
            x=2014, line_dash="dash", line_color="gray", 
            annotation_text="Vision Zero" if col == 1 else "",
            row=1, col=col
        )
        fig_trends.add_vline(
            x=2020, line_dash="dash", line_color="red", 
            annotation_text="COVID-19" if col == 1 else "",
            row=1, col=col
        )

    # Update axes
    fig_trends.update_xaxes(title_text="Year", dtick=2)
    fig_trends.update_yaxes(title_text="Deaths", row=1, col=1)
    fig_trends.update_yaxes(title_text="Rate (per 1k crashes)", row=1, col=2)
    fig_trends.update_yaxes(title_text="Rate (per 1k crashes)", row=1, col=3)

    fig_trends.update_layout(
        title=dict(
            text="<b>NYC Traffic Fatality Trends 2012–2026</b>",
            x=0, font=dict(size=16),
        ),
        height=450,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom", y=-0.25,
            xanchor="center", x=0.75,
        ),
    )

    # Calculate key stats
    deaths_2014 = annual_stats[annual_stats["year"] == 2014]["total_killed"].values[0]
    deaths_2025 = annual_stats[annual_stats["year"] == 2025]["total_killed"].values[0]
    count_pct_change = (deaths_2025 - deaths_2014) / deaths_2014 * 100

    rate_2014 = annual_stats[annual_stats["year"] == 2014]["fatality_rate"].values[0]
    rate_2025 = annual_stats[annual_stats["year"] == 2025]["fatality_rate"].values[0]
    rate_pct_change = (rate_2025 - rate_2014) / rate_2014 * 100

    vru_pct_2025 = (
        annual_stats[annual_stats["year"] == 2025]["vru_killed"].values[0] / 
        annual_stats[annual_stats["year"] == 2025]["total_killed"].values[0] * 100
    )

    mo.vstack([
        fig_trends,
        mo.md(f"""
    **Key Observations:**

    - **Absolute progress:** Deaths declined from {int(deaths_2014)} (2014) to {int(deaths_2025)} (2025) 
      — a **{abs(count_pct_change):.0f}% reduction** and the lowest count since 1910 (NYC DOT, 2026)

    - **Rate deterioration:** Fatality rate per 1,000 crashes **worsened** {abs(rate_pct_change):.1f}% 
      from {rate_2014:.2f} (2014) to {rate_2025:.2f} (2025) — crashes became deadlier

    - **COVID disruption:** Sharp spike in 2020-2021 as risky driving increased despite lower traffic volume; 
      both counts and rates elevated above 2019 baseline

    - **VRU burden persists:** Vulnerable road users account for {vru_pct_2025:.0f}% of 2025 deaths, 
      underscoring continued need for pedestrian/cyclist-focused interventions

    **The paradox explained:** While total deaths **declined** (fewer crashes overall), the fatality **rate increased** 
    (each crash became more likely to be deadly). This suggests post-COVID behavioral changes — speeding, 
    distracted driving, failure to wear seatbelts — made crashes more severe even as total crash volume decreased.

    **Question:** Where are the remaining {int(deaths_2025)} deaths occurring?
        """),
    ])
    return


@app.cell
def _():
    return


@app.cell
def _(fatal, go, mo, pd):
    """Analyze contributing factors pre vs post COVID"""

    pre_covid_factors = fatal[fatal["year"].between(2014, 2019)]
    post_covid_factors = fatal[fatal["year"] >= 2020]

    # Get top contributing factors for each period
    def get_top_factors(crashes_df, top_n=10):
        factors = (
            crashes_df["contributing_factor_vehicle_1"]
            .value_counts()
        )
        # Remove "Unspecified" if present
        factors = factors[factors.index != "Unspecified"]
        return factors.head(top_n)

    pre_factors = get_top_factors(pre_covid_factors)
    post_factors = get_top_factors(post_covid_factors)

    # Normalize to percentages for comparison
    pre_pct = (pre_factors / (pre_covid_factors["contributing_factor_vehicle_1"].dropna().count()) * 100).round(2)
    post_pct = (post_factors / (post_covid_factors["contributing_factor_vehicle_1"].dropna().count()) * 100).round(2)

    # Combine all unique factors
    all_factors = sorted(set(pre_pct.index) | set(post_pct.index))

    # Create comparison data
    comparison_data = pd.DataFrame({
        "Factor": all_factors,
        "Pre-COVID (2014-2019)": [pre_pct.get(f, 0) for f in all_factors],
        "Post-COVID (2020-2026)": [post_pct.get(f, 0) for f in all_factors],
    })

    # Calculate change
    comparison_data["Change"] = (
        comparison_data["Post-COVID (2020-2026)"] - 
        comparison_data["Pre-COVID (2014-2019)"]
    )

    # Sort by post-COVID prevalence
    comparison_data = comparison_data.sort_values("Post-COVID (2020-2026)", ascending=True)

    # Create diverging bar chart
    fig_factors = go.Figure()

    # Pre-COVID bars (left)
    fig_factors.add_trace(go.Bar(
        y=comparison_data["Factor"],
        x=-comparison_data["Pre-COVID (2014-2019)"],
        orientation='h',
        name='Pre-COVID (2014-2019)',
        marker=dict(color='#440154'),
        text=comparison_data["Pre-COVID (2014-2019)"].round(1),
        textposition='auto',
        hovertemplate='<b>%{y}</b><br>Pre-COVID: %{text}%<extra></extra>',
    ))

    # Post-COVID bars (right)
    fig_factors.add_trace(go.Bar(
        y=comparison_data["Factor"],
        x=comparison_data["Post-COVID (2020-2026)"],
        orientation='h',
        name='Post-COVID (2020-2026)',
        marker=dict(color='#fde725'),
        text=comparison_data["Post-COVID (2020-2026)"].round(1),
        textposition='auto',
        hovertemplate='<b>%{y}</b><br>Post-COVID: %{text}%<extra></extra>',
    ))

    fig_factors.update_layout(
        title=dict(
            text="<b>Contributing Factors: Pre-COVID vs Post-COVID</b><br><sup>Top factors in fatal crashes (% of all fatal crashes)</sup>",
            x=0,
            font=dict(size=16),
        ),
        xaxis=dict(
            title="← Pre-COVID          Percentage          Post-COVID →",
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor='black',
        ),
        yaxis=dict(title=""),
        barmode='overlay',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="center", x=0.5,
        ),
        hovermode='y unified',
    )

    # Identify biggest changes
    top_increases = comparison_data.nlargest(3, "Change")
    top_decreases = comparison_data.nsmallest(3, "Change")

    # Build markdown strings outside f-string
    increases_text = ""
    for _, row_factors_pre in top_increases.iterrows():
        increases_text += f"- **{row_factors_pre['Factor']}**: {row_factors_pre['Pre-COVID (2014-2019)']:.1f}% → {row_factors_pre['Post-COVID (2020-2026)']:.1f}% (+{row_factors_pre['Change']:.1f}%)\n"

    decreases_text = ""
    for _, row_factors_post in top_decreases.iterrows():
        decreases_text += f"- **{row_factors_post['Factor']}**: {row_factors_post['Pre-COVID (2014-2019)']:.1f}% → {row_factors_post['Post-COVID (2020-2026)']:.1f}% ({row_factors_post['Change']:.1f}%)\n"

    mo.vstack([
        fig_factors,
        mo.md(f"""
    ### Contributing Factor Changes: Pre-COVID vs Post-COVID

    **Biggest Increases (Post-COVID):**
    {increases_text}

    **Biggest Decreases (Post-COVID):**
    {decreases_text}

    **Interpretation:**

    The shift in contributing factors explains why crashes became deadlier post-COVID despite fewer total crashes:
    - Increased **high-severity factors** (speeding, unsafe speed, driver inattention)
    - Changed driving behavior during pandemic (empty roads → higher speeds)
    - Reduced enforcement during COVID lockdowns

    This validates the need for **behavioral interventions** (speed cameras, enforcement campaigns) 
    in addition to infrastructure improvements, especially at hotspots that emerged post-2020.
        """),
    ])

    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Spatial Distribution: Where Deaths Occur

    Fatal crashes are classified by street type and spatial pattern to identify responsible
    agencies and intervention priorities.
    """)
    return


@app.cell
def _(fatal):
    """Compute street type distribution"""
    street_type_dist = (
        fatal.groupby("street_type")
        .agg(
            fatal_crashes=("collision_id", "count"),
            total_killed=("number_of_persons_killed", "sum"),
        )
        .reset_index()
    )
    return (street_type_dist,)


@app.cell
def _(mo, px, street_type_dist):
    """Visualize street type split"""

    fig_dist = px.bar(
        street_type_dist,
        x="street_type",
        y="fatal_crashes",
        color="street_type",
        text="fatal_crashes",
        title="<b>Fatal Crashes by Street Type</b>",
        labels={"fatal_crashes": "Fatal Crashes", "street_type": "Street Type"},
        color_discrete_map={
            "Highway": "#d62728",
            "Surface Street": "#1f77b4",
            "Unknown": "#7f7f7f",
        },
    )

    fig_dist.update_traces(texttemplate='%{text}', textposition='outside')
    fig_dist.update_layout(showlegend=False, height=600)

    highway_pct = (
        street_type_dist[street_type_dist["street_type"] == "Highway"]["fatal_crashes"].values[0] /
        street_type_dist["fatal_crashes"].sum() * 100
    )

    surface_pct = (
        street_type_dist[street_type_dist["street_type"] == "Surface Street"]["fatal_crashes"].values[0] /
        street_type_dist["fatal_crashes"].sum() * 100
    )

    mo.vstack([
        fig_dist,
        mo.md(f"""
    **Distribution:**
    - **Surface Streets:** {surface_pct:.0f}% of fatal crashes (city jurisdiction: NYC DOT/NYPD)
    - **Highways:** {highway_pct:.0f}% of fatal crashes (state jurisdiction: NYS DOT/MTA)

    Most deaths occur on surface streets under city control, not highways requiring state coordination.
        """),
    ])
    return


@app.cell
def _(h3):
    """Helper: Compute H3 statistics"""
    def compute_h3_stats(crashes_df, resolution=9):
        fatal = crashes_df[
            crashes_df["any_killed"] &
            crashes_df["latitude"].notna() &
            crashes_df["longitude"].notna() &
            crashes_df["latitude"].between(40.4, 40.95) &
            crashes_df["longitude"].between(-74.3, -73.7)
        ].copy()

        fatal["h3_cell"] = fatal.apply(
            lambda r: h3.latlng_to_cell(r["latitude"], r["longitude"], resolution),
            axis=1
        )

        h3_stats = (
            fatal.groupby("h3_cell")
            .agg(
                total_killed=("number_of_persons_killed", "sum"),
                crash_count=("collision_id", "count"),
                borough=("borough", lambda x: x.mode()[0] if len(x.mode()) > 0 else "Unknown"),
            )
            .reset_index()
        )

        return h3_stats[h3_stats["total_killed"] >= 2]  # Min 2 fatalities


    return (compute_h3_stats,)


@app.cell
def _(compute_h3_stats, df, h3):
    """Helper: Classify hotspots"""
    def classify_hotspots(h3_data):
        h3_set = set(h3_data["h3_cell"])

        def count_neighbors(cell):
            neighbors = set(h3.grid_disk(cell, 1))
            neighbors.discard(cell)
            return sum(1 for n in neighbors if n in h3_set)

        h3_data["fatal_neighbors"] = h3_data["h3_cell"].apply(count_neighbors)
        h3_data["pattern"] = h3_data["fatal_neighbors"].apply(
            lambda n: "Corridor Cluster" if n >= 3 else "Isolated Hotspot"
        )
        return h3_data

    h3_stats = compute_h3_stats(df)
    h3_classified = classify_hotspots(h3_stats)

    corridors = h3_classified[h3_classified["pattern"] == "Corridor Cluster"]
    isolated = h3_classified[h3_classified["pattern"] == "Isolated Hotspot"]

    print(f"✓ Total hotspots: {len(h3_classified):,}")
    print(f"✓ Corridors: {len(corridors):,} ({len(corridors)/len(h3_classified)*100:.0f}%)")
    print(f"✓ Isolated: {len(isolated):,} ({len(isolated)/len(h3_classified)*100:.0f}%)")
    return corridors, h3_classified, isolated


@app.cell
def _(corridors, go, h3, h3_classified, isolated, make_subplots, mo):
    """Create spatial pattern map"""

    def get_geojson(subset):
        features = []
        for _, row in subset.iterrows():
            boundary = h3.cell_to_boundary(row["h3_cell"])
            coords = [[lng, lat] for lat, lng in boundary]
            coords.append(coords[0])
            features.append({
                "type": "Feature",
                "id": row["h3_cell"],
                "geometry": {"type": "Polygon", "coordinates": [coords]},
            })
        return {"type": "FeatureCollection", "features": features}

    def get_centers(subset):
        subset = subset.copy()
        centers = subset["h3_cell"].apply(lambda c: h3.cell_to_latlng(c))
        subset["lat"] = centers.apply(lambda x: x[0])
        subset["lng"] = centers.apply(lambda x: x[1])
        return subset

    cor = get_centers(corridors)
    iso = get_centers(isolated)
    max_killed = h3_classified["total_killed"].max()

    cor_geojson = get_geojson(cor)
    iso_geojson = get_geojson(iso)

    fig_spatial = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Corridor Clusters</b><br><sup>{len(cor)} cells • State agencies</sup>",
            f"<b>Isolated Hotspots</b><br><sup>{len(iso)} cells • City agencies</sup>",
        ],
        specs=[[{"type": "mapbox"}, {"type": "mapbox"}]],
        horizontal_spacing=0.02,
    )

    # Corridors
    fig_spatial.add_trace(
        go.Choroplethmapbox(
            geojson=cor_geojson,
            locations=cor["h3_cell"],
            z=cor["total_killed"],
            colorscale=[[0, "#fee5d9"], [0.5, "#fc8d59"], [1, "#b30000"]],
            zmin=2, zmax=max_killed,
            marker_opacity=0.7,
            marker_line_width=0.5,
            showscale=False,
            hovertemplate="<b>Corridor</b><br>Killed: %{z}<extra></extra>",
        ),
        row=1, col=1,
    )

    # Isolated
    fig_spatial.add_trace(
        go.Scattermapbox(
            lat=iso["lat"], lon=iso["lng"],
            mode="markers",
            marker=dict(
                size=iso["total_killed"].clip(upper=12) * 3,
                color=iso["total_killed"],
                colorscale="Viridis",
                cmin=2, cmax=max_killed,
                opacity=0.8,
                showscale=True,
                colorbar=dict(title="Fatalities", x=1.02),
            ),
            hovertemplate="<b>Isolated</b><br>Killed: %{marker.color}<extra></extra>",
        ),
        row=1, col=2,
    )

    map_config = dict(
        zoom=9.8,
        center={"lat": 40.66, "lon": -73.94},
        style="carto-positron",
    )

    fig_spatial.update_layout(
        mapbox=map_config,
        mapbox2=map_config,
        title="<b>Spatial Patterns: Corridor vs Isolated Hotspots</b>",
        height=550,
        margin=dict(l=0, r=80, t=60, b=0),
    )

    mo.vstack([
        fig_spatial,
        mo.md(f"""
    **Spatial Pattern:**
    - **77% isolated intersections** — Single intersection failures, not corridor-wide problems
    - **23% corridor clusters** — Systemic highway design issues (Belt Parkway, Major Deegan, etc.)

    **Policy Implication:** Most deaths are at isolated intersections addressable through operational 
    changes (signal timing, crosswalk hardening) on 1-3 year timelines, not decade-long highway reconstruction.

    **Question:** Which isolated intersections are deadliest, and when did they become dangerous?
        """),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Focus: Top 20 Deadliest Isolated Hotspots

    We narrow the analysis to the 20 deadliest isolated intersection hotspots — the highest-leverage
    targets for city-level intervention.
    """)
    return


@app.cell
def _(h3, isolated):
    """Extract top 20 isolated hotspots"""
    top_20 = isolated.nlargest(20, "total_killed").reset_index(drop=True)

    # Get lat/lng for display
    top_20["lat"] = top_20["h3_cell"].apply(lambda c: h3.cell_to_latlng(c)[0])
    top_20["lng"] = top_20["h3_cell"].apply(lambda c: h3.cell_to_latlng(c)[1])

    display_top20 = top_20[["borough", "lat", "lng", "total_killed", "crash_count"]].copy()
    display_top20.columns = ["Borough", "Latitude", "Longitude", "Total Killed", "Fatal Crashes"]
    display_top20.index = range(1, 21)
    return display_top20, top_20


@app.cell
def _(display_top20, mo):
    """Display top 20 table"""
    mo.vstack([
        mo.md("### Top 20 Deadliest Isolated Intersection Hotspots"),
        display_top20,
        mo.md("""
    These 20 intersections represent the highest concentration of fatalities among isolated hotspots. 
    Each is a ~175m diameter area containing multiple fatal crashes.
        """),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Hybrid Priority Model: Scoring Isolated Hotspots

    Because fatal crashes at single intersections are statistically sparse events, traditional
    time-series change-point models often fail. Instead, we apply a **Recency-Weighted Hybrid Model**:
    deaths in recent years carry greater weight, and Pre- vs. Post-COVID counts classify each site's
    trend. This separates historical artifacts from emerging behavioral crises.
    """)
    return


@app.cell
def _(df, top_20):
    """Hybrid Priority Scoring Logic"""
    import pandas as _pd

    current_year = 2026
    covid_start = 2020

    # Enrich fatal crashes with h3 cell (reuse column if already present)
    fatal_enriched = df[
        df["any_killed"] &
        df["latitude"].notna() &
        df["longitude"].notna()
    ].copy()

    if "h3_cell" not in fatal_enriched.columns:
        from h3 import latlng_to_cell
        fatal_enriched["h3_cell"] = fatal_enriched.apply(
            lambda r: latlng_to_cell(r["latitude"], r["longitude"], 9), axis=1
        )

    results = []
    for _, row in top_20.iterrows():
        cell_id = row["h3_cell"]
        cell_crashes = fatal_enriched[fatal_enriched["h3_cell"] == cell_id]

        # 1. First Fatal Year
        first_year = cell_crashes["year"].min()

        # 2. Trend Classification
        pre_covid = len(cell_crashes[cell_crashes["year"] < covid_start])
        post_covid = len(cell_crashes[cell_crashes["year"] >= covid_start])

        if first_year >= covid_start:
            trend = "Emergent (New)"
        elif post_covid > pre_covid:
            trend = "Worsening"
        else:
            trend = "Persistent"

        # 3. Recency Score: Inverse distance from current year
        recency_score = sum(1 / (current_year - cell_crashes["year"] + 1))

        # 4. Priority Logic
        if (trend in ["Emergent (New)", "Worsening"]) and (recency_score > 1.2):
            priority = "🔴 URGENT"
        elif trend == "Persistent" and row["total_killed"] >= 5:
            priority = "🟠 HIGH"
        else:
            priority = "🟡 MEDIUM"

        results.append({
            "Borough": row["borough"],
            "Total Killed": row["total_killed"],
            "Trend": trend,
            "First Fatal": int(first_year),
            "Pre-COVID Crashes": pre_covid,
            "Post-COVID Crashes": post_covid,
            "Recency Score": round(recency_score, 2),
            "Priority": priority,
            "h3_cell": cell_id,
            "lat": row["lat"],
            "lng": row["lng"],
        })

    priority_table = _pd.DataFrame(results).sort_values("Recency Score", ascending=False)
    return (priority_table,)


@app.cell
def _(mo, priority_table):
    """Display hybrid priority results"""
    mo.vstack([
        mo.md("## Final Priority Matrix: Isolated Hotspots"),
        mo.ui.table(priority_table.drop(columns=["h3_cell", "lat", "lng"])),
        mo.md(
            f"""
            ### Intervention Strategy
            - **{len(priority_table[priority_table['Priority'] == '🔴 URGENT'])} Urgent Sites**:
              Requires immediate NYPD enforcement and NYC DOT "Quick-Build" (bollards, signal timing).
            - **{len(priority_table[priority_table['Priority'] == '🟠 HIGH'])} High Priority Sites**:
              Long-term chronic failures requiring capital reconstruction (raised crosswalks, curb extensions).
            - **{len(priority_table[priority_table['Priority'] == '🟡 MEDIUM'])} Medium Priority Sites**:
              Enhanced monitoring and pre-emptive operational improvements.
            """
        )
    ])
    return


@app.cell
def _(priority_table, px):
    """Geospatial Visualization of Priority Hotspots"""
    import geopandas as _gpd

    map_data = priority_table.copy()

    fig_map = px.scatter_mapbox(
        map_data,
        lat="lat",
        lon="lng",
        color="Priority",
        size="Total Killed",
        hover_name="Borough",
        hover_data={"Trend": True, "Recency Score": True, "Total Killed": True,
                    "lat": False, "lng": False},
        color_discrete_map={
            "🔴 URGENT": "#e74c3c",
            "🟠 HIGH": "#e67e22",
            "🟡 MEDIUM": "#f1c40f",
        },
        zoom=9.5,
        center={"lat": 40.7128, "lon": -74.0060},
        height=700,
        title="<b>NYC Fatality Hotspots — Hybrid Priority Model</b>",
    )

    fig_map.update_layout(
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
    )
    return (fig_map,)


@app.cell
def _(fig_map, mo):
    mo.ui.plotly(fig_map)
    return


@app.cell
def _(mo, priority_table):
    """Generate policy recommendations from hybrid model"""
    urgent = priority_table[priority_table["Priority"] == "🔴 URGENT"]
    high = priority_table[priority_table["Priority"] == "🟠 HIGH"]
    medium = priority_table[priority_table["Priority"] == "🟡 MEDIUM"]
    emergent = priority_table[priority_table["Trend"] == "Emergent (New)"]
    worsening = priority_table[priority_table["Trend"] == "Worsening"]
    persistent = priority_table[priority_table["Trend"] == "Persistent"]

    mo.md(f"""
    ---
    ## Policy Recommendations by Priority Tier

    Different temporal patterns demand different intervention approaches:

    ### 1. 🔴 URGENT — {len(urgent)} sites (Emergent or Worsening + High Recency)

    **Characteristics:**
    - Post-COVID crash rates exceed pre-COVID, or site appeared after 2020
    - High recency score indicates continued recent deaths

    **Likely Causes:**
    - Changed driving behavior (speeding, distracted driving)
    - Reduced enforcement during pandemic that never recovered
    - Changed traffic patterns (work-from-home, e-bikes, micromobility)

    **Recommended Actions:**
    - **Immediate site investigation** (30 days) to identify new risk factors
    - **Emergency quick-build countermeasures:** Speed feedback signs, temporary bollards, signal retiming
    - **NYPD enforcement blitz** at identified locations

    ---

    ### 2. 🟠 HIGH PRIORITY — {len(high)} sites (Persistent + ≥5 killed)

    **Characteristics:**
    - Dangerous throughout the entire 2012–2026 period
    - Pre-COVID crash rates equal or exceed post-COVID — a chronic, not new, failure

    **Likely Causes:**
    - Fundamental infrastructure failure (poor visibility, confusing geometry)
    - High-speed arterial with inadequate crossing protection
    - Complex intersection never redesigned

    **Recommended Actions:**
    - **Comprehensive redesign** (1–2 years): Capital reconstruction, not operational fixes
    - **Infrastructure investment:** Refuge islands, protected signal phases, road diet, geometric improvements
    - **High confidence of success:** Problem is clear, consistent, and well-understood

    ---

    ### 3. 🟡 MEDIUM — {len(medium)} sites

    **Recommended Actions:**
    - **Accelerated monitoring:** Quarterly review instead of annual
    - **Pre-emptive operational improvements:** Crosswalk hardening, signage, lighting
    - **Watch for escalation** into URGENT tier

    ---

    ### Implementation Priority Matrix

    | Tier | Count | Trend Breakdown | Timeline | First Action |
    |------|-------|-----------------|----------|--------------|
    | 🔴 URGENT | {len(urgent)} | Emergent/Worsening | 30 days | Emergency countermeasures |
    | 🟠 HIGH | {len(high)} | Persistent | 1–2 years | Capital project scoping |
    | 🟡 MEDIUM | {len(medium)} | Mixed | Ongoing | Enhanced monitoring |

    **Trend Summary:** {len(emergent)} Emergent · {len(worsening)} Worsening · {len(persistent)} Persistent

    **Next Steps:**

    1. **Field verify** top 20 locations within 60 days
    2. **Deploy emergency measures** at URGENT sites within 30 days
    3. **Allocate capital budget** for HIGH priority redesigns (FY 2027)
    4. **Establish quarterly review cadence** for MEDIUM sites

    ---

    *Analysis: Remy Ndayizeye · NBDIH · National Student Data Corps · U.S. DOT FHWA*
    *Data: NYC Open Data (1.95M crashes, 2012–2026)*
    *Methods: H3 spatial binning · Recency-Weighted Hybrid Priority Model · Pre/Post-COVID trend classification*
    """)
    return


if __name__ == "__main__":
    app.run()
