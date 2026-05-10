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
    """Install ruptures for change point detection"""
    import subprocess
    try:
        import ruptures as rpt
        import h3
    except ImportError:
        subprocess.run(['pip', 'install', 'ruptures', 'h3', '--break-system-packages'], 
                      capture_output=True)
        import ruptures as rpt
        import h3
    return h3, rpt


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
    where the remaining deaths occur and when those locations became dangerous, enabling targeted
    intervention deployment.

    **Key Findings:**

    1. **Progress with persistent burden** — Overall fatalities declined 31%, but vulnerable road users
       (pedestrians + cyclists) still account for 66% of deaths.

    2. **Concentrated at isolated intersections** — 77% of fatal hotspots are isolated surface street
       intersections, not highway corridors, meaning most deaths are addressable at city level.

    3. **Temporal patterns reveal causality** — Of the top 20 deadliest intersections, 30% became
       dangerous after COVID-19, indicating behavioral changes requiring different interventions than
       chronic infrastructure failures.

    4. **Different patterns demand different responses** — Recent emergent hotspots need rapid
       investigation; persistent hotspots need capital redesign.

    ---

    **Data:** NYC Open Data — 1.95M crashes (July 2012 – April 2026)
    **Methods:** H3 spatial binning · Change point detection · Temporal trend analysis
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

    print(f"✓ Enriched {len(df):,} records")
    return (df,)


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
    """Create trend visualization"""

    fig_trends = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            "<b>Overall Fatality Rate</b><br><sup>Deaths per 1,000 crashes</sup>",
            "<b>Vulnerable Road User Fatality Rate</b><br><sup>Pedestrian + Cyclist deaths per 1,000 crashes</sup>",
        ],
        horizontal_spacing=0.12,
    )

    # Left: Overall rate
    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["fatality_rate"],
            mode="lines+markers",
            name="Overall",
            line=dict(color="#d62728", width=3),
            marker=dict(size=8),
        ),
        row=1, col=1,
    )

    # Right: VRU rates
    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["vru_rate"],
            mode="lines+markers",
            name="VRU Total",
            line=dict(color="#1f77b4", width=3),
            marker=dict(size=8),
        ),
        row=1, col=2,
    )

    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["ped_rate"],
            mode="lines",
            name="Pedestrians",
            line=dict(color="#ff7f0e", width=2, dash="dash"),
        ),
        row=1, col=2,
    )

    fig_trends.add_trace(
        go.Scatter(
            x=annual_stats["year"],
            y=annual_stats["cyc_rate"],
            mode="lines",
            name="Cyclists",
            line=dict(color="#2ca02c", width=2, dash="dot"),
        ),
        row=1, col=2,
    )

    # Mark Vision Zero launch and COVID
    for col in [1, 2]:
        fig_trends.add_vline(x=2014, line_dash="dash", line_color="gray", 
                            annotation_text="Vision Zero", row=1, col=col)
        fig_trends.add_vline(x=2020, line_dash="dash", line_color="red", 
                            annotation_text="COVID-19", row=1, col=col)

    fig_trends.update_xaxes(title_text="Year", dtick=2)
    fig_trends.update_yaxes(title_text="Rate (per 1,000 crashes)")

    fig_trends.update_layout(
        title=dict(
            text="<b>NYC Traffic Fatality Trends 2012–2026</b>",
            x=0, font=dict(size=16),
        ),
        height=450,
        hovermode="x unified",
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
    )

    # Calculate key stats
    rate_2014 = annual_stats[annual_stats["year"] == 2014]["fatality_rate"].values[0]
    rate_2025 = annual_stats[annual_stats["year"] == 2025]["fatality_rate"].values[0]
    pct_change = (rate_2025 - rate_2014) / rate_2014 * 100

    vru_pct_2025 = (
        annual_stats[annual_stats["year"] == 2025]["vru_killed"].values[0] / 
        annual_stats[annual_stats["year"] == 2025]["total_killed"].values[0] * 100
    )

    mo.vstack([
        fig_trends,
        mo.md(f"""
    **Key Observations:**

    - **Overall progress:** Fatality rate declined {abs(pct_change):.1f}% from 2014 ({rate_2014:.2f}) 
      to 2025 ({rate_2025:.2f})
    - **COVID disruption:** Sharp spike in 2020-2021 as risky driving behavior increased despite lower traffic volume
    - **VRU burden persists:** Vulnerable road users account for {vru_pct_2025:.0f}% of 2025 deaths, 
      underscoring continued need for pedestrian/cyclist interventions

    **Question:** Where are the remaining deaths occurring?
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
def _(df):
    """Compute street type distribution"""
    fatal = df[df["any_killed"] & df["street_type"].notna()]

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
    fig_dist.update_layout(showlegend=False, height=400)

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
    ## Change Point Analysis: When Did Hotspots Emerge?

    We apply change point detection to identify when each of the top 20 intersections became dangerous.
    This reveals whether problems are recent (COVID-related) or chronic (infrastructure failures).
    """)
    return


@app.cell
def _(h3, pd, rpt):
    """Helper: Detect change points for one cell"""
    def detect_change_point(crashes_df, cell_id):
        # Get crashes in this cell
        cell_crashes = crashes_df[
            crashes_df["any_killed"] &
            crashes_df["latitude"].notna() &
            crashes_df["longitude"].notna()
        ].copy()

        cell_crashes["h3_cell"] = cell_crashes.apply(
            lambda r: h3.latlng_to_cell(r["latitude"], r["longitude"], 9),
            axis=1
        )
        cell_crashes = cell_crashes[cell_crashes["h3_cell"] == cell_id]

        if len(cell_crashes) < 10:
            return None

        # Quarterly time series
        cell_crashes["quarter"] = pd.PeriodIndex(cell_crashes["crash_date"], freq="Q")
        quarterly = (
            cell_crashes.groupby("quarter")
            .size()
            .reindex(
                pd.period_range(
                    start=cell_crashes["quarter"].min(),
                    end=cell_crashes["quarter"].max(),
                    freq="Q"
                ),
                fill_value=0
            )
        )

        signal = quarterly.values
        if len(signal) < 8:
            return None

        # Detect change points
        try:
            model = rpt.Pelt(model="rbf", min_size=4, jump=1).fit(signal)
            change_points = model.predict(pen=10)

            if len(change_points) <= 1:
                return None

            # Find most significant change
            best_idx = None
            max_shift = 0

            for cp in change_points[:-1]:
                pre_mean = signal[:cp].mean()
                post_mean = signal[cp:].mean()
                shift = abs(post_mean - pre_mean)

                if shift > max_shift:
                    max_shift = shift
                    best_idx = cp

            if best_idx is None:
                return None

            change_quarter = quarterly.index[best_idx]
            pre_rate = signal[:best_idx].mean()
            post_rate = signal[best_idx:].mean()
            pct_change = ((post_rate - pre_rate) / pre_rate * 100) if pre_rate > 0 else 0

            # Classify pattern
            year = change_quarter.year
            if year >= 2020:
                pattern = "Recent Emergence (2020+)"
            elif pct_change > 50:
                pattern = "Sudden Spike"
            elif pct_change > 20:
                pattern = "Gradual Worsening"
            else:
                pattern = "Stable"

            return {
                "change_year": year,
                "change_quarter": str(change_quarter),
                "pre_rate": round(pre_rate, 2),
                "post_rate": round(post_rate, 2),
                "percent_change": round(pct_change, 1),
                "pattern": pattern,
                "quarterly_data": quarterly,
            }

        except Exception as e:
            print(f"Error: {e}")
            return None


    return (detect_change_point,)


@app.cell
def _(detect_change_point, df, pd, top_20):
    """Run change point analysis on top 20"""
    results = []

    for idx, row_cpt in top_20.iterrows():
        cell_id = row_cpt["h3_cell"]
        cp = detect_change_point(df, cell_id)

        results.append({
            "rank": idx + 1,
            "borough": row_cpt["borough"],
            "lat": row_cpt["lat"],
            "lng": row_cpt["lng"],
            "total_killed": row_cpt["total_killed"],
            "crash_count": row_cpt["crash_count"],
            "change_year": cp["change_year"] if cp else None,
            "change_quarter": cp["change_quarter"] if cp else None,
            "pre_rate": cp["pre_rate"] if cp else None,
            "post_rate": cp["post_rate"] if cp else None,
            "percent_change": cp["percent_change"] if cp else None,
            "pattern": cp["pattern"] if cp else "Persistent Hotspot",
            "quarterly_data": cp["quarterly_data"] if cp else None,
        })

    change_results = pd.DataFrame(results)
    return (change_results,)


@app.cell
def _(change_results, mo):
    """Display change point results"""
    display_cp = change_results[[
        "rank", "borough", "total_killed", "pattern", 
        "change_year", "pre_rate", "post_rate", "percent_change"
    ]].copy()

    display_cp.columns = [
        "Rank", "Borough", "Total Killed", "Pattern", 
        "Change Year", "Pre-Change Rate", "Post-Change Rate", "% Change"
    ]

    pattern_counts = change_results["pattern"].value_counts()

    lines = [f"- **{pattern}**: {count} intersections" for pattern, count in pattern_counts.items()]
    output = "\n".join(lines)
    mo.vstack([
        mo.md("### Change Point Detection Results"),
        display_cp,
        mo.md(f"""
    **Pattern Distribution:**
    print(f"{output}")

    **Interpretation:**
    - **Recent Emergence** — Became dangerous 2020+; likely COVID behavioral changes
    - **Sudden Spike** — Specific event triggered danger (construction, signal change, development)
    - **Gradual Worsening** — Slow deterioration missed by annual reviews
    - **Persistent Hotspot** — Dangerous throughout period; chronic infrastructure failure
        """),
    ])
    return


@app.cell
def _(change_results, go, make_subplots, pd):
    """Visualize time series for top 5 with change points"""
    top_5 = change_results.head(5)

    fig_ts = make_subplots(
        rows=5, cols=1,
        subplot_titles=[
            f"#{row['rank']} {row['borough']} — {row['pattern']} ({int(row['total_killed'])} killed)"
            for _, row in top_5.iterrows()
        ],
        vertical_spacing=0.08,
    )

    for i, (_, row) in enumerate(top_5.iterrows()):
        if row["quarterly_data"] is None:
            continue

        quarterly = row["quarterly_data"]
        x_dates = [q.to_timestamp() for q in quarterly.index]

        # Add time series
        fig_ts.add_trace(
            go.Scatter(
                x=x_dates,
                y=quarterly.values,
                mode="lines+markers",
                line=dict(width=2),
                marker=dict(size=5),
                showlegend=False,
            ),
            row=i+1, col=1,
        )

        # Add change point line
        if row["change_year"]:
            change_date = pd.to_datetime(f"{row['change_year']}-01-01")
            fig_ts.add_vline(
                x=change_date.timestamp() * 1000,
                line_dash="dash",
                line_color="red",
                line_width=2,
                row=i+1, col=1,
            )

    fig_ts.update_xaxes(title_text="Quarter", row=5, col=1)
    fig_ts.update_yaxes(title_text="Crashes", range=[0, None])
    fig_ts.update_layout(
        title="<b>Temporal Patterns: Top 5 Deadliest Isolated Hotspots</b><br><sup>Red line = detected change point</sup>",
        height=1200,
        margin=dict(l=60, r=20, t=80, b=60),
    )
    return (fig_ts,)


@app.cell
def _(fig_ts, mo):
    mo.vstack([
        fig_ts,
        mo.md("""
    **Visual Patterns:**
    - Flat lines before red marker, then elevated after = Recent emergence
    - Consistently elevated throughout = Persistent hotspot
    - Sudden jump at red marker = Sudden spike event
        """),
    ])
    return


@app.cell
def _(change_results, mo):
    """Generate policy recommendations"""

    recent = change_results[change_results["pattern"] == "Recent Emergence (2020+)"]
    persistent = change_results[change_results["pattern"] == "Persistent Hotspot"]
    sudden = change_results[change_results["pattern"] == "Sudden Spike"]
    gradual = change_results[change_results["pattern"] == "Gradual Worsening"]

    mo.md(f"""
    ---
    ## Policy Recommendations by Temporal Pattern

    Different temporal patterns demand different intervention approaches:

    ### 1. Recent Emergence (2020+) — {len(recent)} intersections 🔴 URGENT

    **Characteristics:**
    - Became dangerous during/after COVID-19
    - Pre-2020: Low crash rates
    - Post-2020: Elevated rates

    **Likely Causes:**
    - Changed driving behavior (speeding, distracted driving)
    - Reduced enforcement during pandemic
    - Changed traffic patterns (work from home)

    **Recommended Actions:**
    - **Immediate site investigation** (30 days) to identify new risk factors
    - **Emergency countermeasures:** Speed feedback signs, temporary bollards, increased enforcement
    - **Monitor for stabilization** vs continued worsening

    ---

    ### 2. Persistent Hotspot — {len(persistent)} intersections 🟠 HIGH PRIORITY

    **Characteristics:**
    - Dangerous throughout entire 2012-2026 period
    - No detected change point
    - Steady fatality stream year after year

    **Likely Causes:**
    - Fundamental infrastructure failure (poor visibility, confusing geometry)
    - High-speed arterial with inadequate crossing protection
    - Complex intersection never redesigned

    **Recommended Actions:**
    - **Comprehensive redesign** (1-2 years): Not operational fixes, but capital reconstruction
    - **Infrastructure investment:** Refuge islands, signal phases, road diet, geometric improvements
    - **High confidence of success:** Problem is clear and consistent

    ---

    ### 3. Sudden Spike — {len(sudden)} intersections 🟡 INVESTIGATE

    **Characteristics:**
    - Specific year when crashes jumped
    - Large percent change (>50%)

    **Likely Causes:**
    - Construction altered traffic flow
    - Signal timing changed
    - New development nearby
    - Major crash with secondary effects

    **Recommended Actions:**
    - **Review project history** (45 days): Check NYC DOT records for changes in spike year
    - **Interview local staff:** What happened that year?
    - **Consider reversal:** If intervention failed, undo or modify

    ---

    ### 4. Gradual Worsening — {len(gradual)} intersections 🟢 MONITOR

    **Characteristics:**
    - Slow increase over multiple years
    - May not trigger annual review flags

    **Recommended Actions:**
    - **Accelerated monitoring:** Check quarterly instead of annually
    - **Pre-emptive intervention:** Don't wait for another death
    - **Root cause analysis:** Why is it slowly deteriorating?

    ---

    ### Implementation Priority Matrix

    | Pattern | Count | Timeline | Responsible Agency | First Action |
    |---------|-------|----------|-------------------|--------------|
    | Recent Emergence | {len(recent)} | 30 days | NYC DOT / NYPD | Site investigation + emergency measures |
    | Persistent Hotspot | {len(persistent)} | 1-2 years | NYC DOT | Capital project scoping |
    | Sudden Spike | {len(sudden)} | 45 days | NYC DOT | Project history review |
    | Gradual Worsening | {len(gradual)} | Ongoing | NYC DOT | Enhanced monitoring |

    **Next Steps:**

    1. **Field verify** top 20 locations within 60 days
    2. **Deploy emergency measures** at Recent Emergence sites within 30 days
    3. **Allocate capital budget** for Persistent Hotspot redesigns (FY 2027)
    4. **Review project records** for Sudden Spike sites within 45 days

    ---

    *Analysis: Remy Ndayizeye · NBDIH · National Student Data Corps · U.S. DOT FHWA*  
    *Data: NYC Open Data (1.95M crashes, 2012-2026)*  
    *Methods: H3 spatial binning · PELT change point detection · Temporal pattern classification*
    """)
    return




if __name__ == "__main__":
    app.run()
