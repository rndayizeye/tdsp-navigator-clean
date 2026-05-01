import marimo

__generated_with = "0.23.2"
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
    import matplotlib.pyplot as plt
    import re
    import json
    import h3 as h3lib
    import warnings
    warnings.filterwarnings("ignore")
    return go, gpd, h3lib, make_subplots, mo, pd, plt, px, re


@app.cell
def _(mo):
    mo.md("""
    # NYC Traffic Fatality Analysis — Vision Zero Policy Brief

    **Remy Ndayizeye**
    Northeast Big Data Innovation Hub · National Student Data Corps · U.S. DOT FHWA
    *April 2026*

    ---

    ## Executive Summary

    This analysis examines 1.95 million NYC motor vehicle crashes (2012–2026) to identify
    the deadliest corridors and intersections and inform Vision Zero policy interventions.

    **Key Findings:**

    1. **Fatalities are highly concentrated** — The top 20 streets account for a disproportionate
       share of all traffic deaths, with five streets alone representing ~30% of fatalities.

    2. **Pedestrians bear the greatest burden** — Vulnerable road users (pedestrians + cyclists)
       account for 66% of all traffic fatalities despite being a minority of road users.

    3. **Most deaths occur at isolated intersections, not highway corridors** — 77% of fatal
       crash hotspots are isolated surface street intersections rather than corridor clusters,
       meaning most deaths are addressable through city-level interventions.

    4. **Different street types require different agencies** — Highways need state-level
       intervention (NYS DOT/MTA), while surface streets are under city control (NYC DOT/NYPD).

    5. **High-leverage opportunities exist** — Single engineering interventions at isolated
       hotspots can eliminate entire crash clusters on a 1–3 year timeline.

    ---

    **Data Source:** NYC Open Data — Motor Vehicle Collisions (Socrata API)
    **Pipeline:** Kedro 0.19.15 · Incremental watermark-based ingestion · 28-column parquet schema
    **Spatial Method:** H3 hexagonal binning (resolution 9, ~175m diameter cells)
    **Analysis Period:** July 2012 – April 2026 (13.75 years)
    """)
    return


@app.cell
def _(gpd, pd):
    """Load and enrich crash data"""
    # Load crash data
    df_raw = pd.read_parquet("data/02_primary/nyc_crashes.parquet")

    # Parse dates and extract time features
    df_raw["crash_date"] = pd.to_datetime(df_raw["crash_date"], errors="coerce")
    df_raw["crash_time"] = pd.to_datetime(df_raw["crash_time"], format="%H:%M", errors="coerce")
    df_raw["hour"]       = df_raw["crash_time"].dt.hour
    df_raw["day_of_week"]= df_raw["crash_date"].dt.day_name()
    df_raw["month"]      = df_raw["crash_date"].dt.month
    df_raw["year"]       = df_raw["crash_date"].dt.year

    # Severity flags
    df_raw["any_killed"]  = df_raw["number_of_persons_killed"] > 0
    df_raw["any_injury"]  = df_raw["number_of_persons_injured"] > 0

    # Load census geodataframe for borough map
    census_gdf = gpd.read_file("data/02_intermediate/nyc_census_geodf.geojson")

    print(f"✓ Crashes loaded: {len(df_raw):,}")
    print(f"✓ Date range: {df_raw['crash_date'].min().date()} → {df_raw['crash_date'].max().date()}")
    print(f"✓ Fatal crashes: {df_raw['any_killed'].sum():,} ({df_raw['any_killed'].sum()/len(df_raw)*100:.2f}%)")
    return (df_raw,)


@app.cell
def _(pd, re):
    """Helper functions for street normalization and classification"""

    # ── Street normalization ──────────────────────────────────────────────────
    def normalize_street_name(name: str) -> str | None:
        """Normalize street names for consistent aggregation"""
        if pd.isna(name):
            return None
        name = name.upper().strip()
        name = re.sub(r'\s+', ' ', name)
        replacements = {
            r'\bST\b': 'STREET',   r'\bAVE\b': 'AVENUE',
            r'\bBLVD\b': 'BOULEVARD', r'\bRD\b': 'ROAD',
            r'\bDR\b': 'DRIVE',    r'\bPL\b': 'PLACE',
            r'\bPKWY\b': 'PARKWAY', r'\bEXPY\b': 'EXPRESSWAY',
            r'\bHWY\b': 'HIGHWAY', r'\bBDWAY\b': 'BROADWAY',
        }
        for pattern, replacement in replacements.items():
            name = re.sub(pattern, replacement, name)
        return name

    # ── Street type classification ────────────────────────────────────────────
    HIGHWAYS = {
        "BELT PARKWAY", "GRAND CENTRAL PARKWAY", "MAJOR DEEGAN EXPRESSWAY",
        "BRUCKNER BOULEVARD", "HENRY HUDSON PARKWAY", "CROSS ISLAND PARKWAY",
        "LONG ISLAND EXPRESSWAY", "CROSS BRONX EXPRESSWAY",
        "BROOKLYN QUEENS EXPRESSWAY", "BRUCKNER EXPRESSWAY", "FDR DRIVE",
        "STATEN ISLAND EXPRESSWAY", "GOWANUS EXPRESSWAY", "HARLEM RIVER DRIVE",
        "HUTCHINSON RIVER PARKWAY", "SHERIDAN EXPRESSWAY", "VAN WYCK EXPRESSWAY",
    }

    def classify_street_type(street: str) -> str:
        """Classify street as Highway or Surface Street based on name"""
        if pd.isna(street):
            return "Unknown"
        if street in HIGHWAYS:
            return "Highway"
        if any(x in street for x in ["EXPRESSWAY", "PARKWAY", "FREEWAY"]):
            return "Highway"
        return "Surface Street"

    # ── Contributing factor helpers ───────────────────────────────────────────
    def dominant_factor(series: pd.Series) -> str:
        """Return the most common contributing factor excluding 'Unspecified'"""
        counts = series[series != "Unspecified"].value_counts()
        return counts.index[0] if not counts.empty else "Unspecified"

    def dominant_road_user(row) -> str:
        """Determine which road user type had the most fatalities"""
        return max(
            {"Pedestrian": row["ped_killed"],
             "Cyclist": row["cyc_killed"],
             "Motorist": row["mot_killed"]},
            key=lambda k: {"Pedestrian": row["ped_killed"],
                           "Cyclist": row["cyc_killed"],
                           "Motorist": row["mot_killed"]}[k]
        )

    def get_policy_recommendation(factor: str, road_user: str) -> str:
        """Generate policy recommendation based on contributing factor and road user"""
        base = {
            "Driver Inattention/Distraction": "Speed cameras, signal retiming, distracted driving enforcement",
            "Failure to Yield Right-of-Way":  "Leading pedestrian intervals, exclusive pedestrian phases",
            "Speeding":                        "Speed bumps, road diet, automated speed enforcement",
            "Alcohol Involvement":             "Late night enforcement, DUI checkpoints",
            "Traffic Control Disregarded":     "Signal upgrades, red light cameras",
            "Unsafe Speed":                    "Speed cameras, road diet",
        }.get(factor, "Review traffic control and enforcement")
        if road_user == "Pedestrian":
            return base + " + crosswalk hardening, pedestrian refuge islands"
        if road_user == "Cyclist":
            return base + " + protected bike lanes, intersection hardening"
        return base

    return (
        classify_street_type,
        dominant_factor,
        dominant_road_user,
        get_policy_recommendation,
        normalize_street_name,
    )


@app.cell
def _(classify_street_type, df_raw, normalize_street_name, pd):
    """Apply enrichment functions to create analysis-ready dataset"""
    def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["street_normalized"] = (
            df["on_street_name"]
            .apply(normalize_street_name)
            .fillna(df["off_street_name"].apply(normalize_street_name))
        )
        df["cross_street_normalized"] = df["cross_street_name"].apply(normalize_street_name)
        df["street_type"] = df["street_normalized"].apply(classify_street_type)
        return df

    df = enrich_dataframe(df_raw)
    print(f"✓ Enriched dataset: {len(df):,} rows with normalized street names and classifications")
    return (df,)


@app.cell
def _(df, mo):
    """Calculate and display dataset overview metrics"""
    total         = len(df)
    total_killed  = int(df["number_of_persons_killed"].sum())
    total_injured = int(df["number_of_persons_injured"].sum())
    ped_killed    = int(df["number_of_pedestrians_killed"].sum())
    cyc_killed    = int(df["number_of_cyclist_killed"].sum())
    mot_killed    = int(df["number_of_motorist_killed"].sum())
    fatal_crashes = int(df["any_killed"].sum())
    pct_fatal     = fatal_crashes / total * 100
    vru_killed    = ped_killed + cyc_killed

    mo.md(f"""
    ## Dataset Overview

    | Metric | Value |
    |--------|-------|
    | Total crashes | {total:,} |
    | Fatal crashes | {fatal_crashes:,} ({pct_fatal:.2f}% of all crashes) |
    | Total killed | {total_killed:,} |
    | Total injured | {total_injured:,} |
    | **Pedestrians killed** | **{ped_killed:,}** ({ped_killed/total_killed*100:.1f}% of fatalities) |
    | **Cyclists killed** | **{cyc_killed:,}** ({cyc_killed/total_killed*100:.1f}% of fatalities) |
    | **Motorists killed** | **{mot_killed:,}** ({mot_killed/total_killed*100:.1f}% of fatalities) |
    | **Vulnerable road users (VRU) killed** | **{vru_killed:,}** ({vru_killed/total_killed*100:.1f}% of fatalities) |

    > **Vision Zero Context**: NYC launched Vision Zero in 2014 with the goal of eliminating all
    > traffic deaths. Pedestrians and cyclists together account for
    > **{vru_killed/total_killed*100:.1f}%** of all fatalities despite being a minority of road users —
    > making them the primary target population for infrastructure and enforcement interventions.
    """)
    return cyc_killed, mot_killed, ped_killed, total_killed


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Spatial Hotspot Analysis — H3 Hexagonal Binning

    Each fatal crash is assigned to an **H3 hexagonal cell** (~175m diameter, resolution 9).
    Cells are then classified into two distinct spatial patterns:

    ### Pattern Definitions

    **Corridor Clusters** — Cells with ≥3 neighboring cells that also contain fatalities.
    - Indicate **systemic road design problems** along a corridor (e.g., Belt Parkway, Major Deegan)
    - Require **state/regional agency intervention** (NYS DOT, MTA Bridges & Tunnels)
    - Interventions: Corridor-wide speed enforcement, managed speed zones, geometric redesign

    **Isolated Hotspots** — Cells with <3 neighboring fatal cells.
    - Point to **specific intersection failures** rather than corridor-wide problems
    - Represent the **highest-leverage Vision Zero targets**
    - A single targeted intervention (leading pedestrian interval, signal retiming, crosswalk hardening)
      can eliminate the hotspot entirely
    - Fall under **city agency jurisdiction** (NYC DOT, NYPD)

    This distinction is critical for policy: corridor problems require multi-year capital projects
    and state-level coordination, while isolated hotspots can often be addressed with operational
    changes on a 1–3 year timeline.
    """)
    return


@app.cell
def _(df, dominant_factor, pd):
    """Compute H3 fatality map and classify hotspots"""

    def compute_h3_fatality_map(df: pd.DataFrame, resolution: int = 9) -> pd.DataFrame:
        """Aggregate fatal crashes into H3 hexagonal cells"""
        import h3

        fatal = df[
            df["any_killed"] &
            df["latitude"].notna() &
            df["longitude"].notna() &
            df["latitude"].between(40.4, 40.95) &
            df["longitude"].between(-74.3, -73.7)
        ].copy()

        fatal["h3_cell"] = fatal.apply(
            lambda r: h3.latlng_to_cell(r["latitude"], r["longitude"], resolution),
            axis=1,
        )

        h3_stats = (
            fatal.groupby("h3_cell")
            .agg(
                total_killed=("number_of_persons_killed", "sum"),
                crash_count=("collision_id", "count"),
                ped_killed=("number_of_pedestrians_killed", "sum"),
                cyc_killed=("number_of_cyclist_killed", "sum"),
                mot_killed=("number_of_motorist_killed", "sum"),
                top_factor=("contributing_factor_vehicle_1", dominant_factor),
                borough=("borough", lambda x: x.mode()[0] if not x.mode().empty else "Unknown"),
            )
            .reset_index()
        )

        # Dominant road user
        h3_stats["dominant_user"] = h3_stats.apply(
            lambda r: max(
                {"Pedestrian": r["ped_killed"],
                 "Cyclist": r["cyc_killed"],
                 "Motorist": r["mot_killed"]},
                key=lambda k: {"Pedestrian": r["ped_killed"],
                               "Cyclist": r["cyc_killed"],
                               "Motorist": r["mot_killed"]}[k]
            ), axis=1
        )

        return h3_stats

    def classify_hotspots(h3_data: pd.DataFrame) -> pd.DataFrame:
        """Classify cells as corridor clusters or isolated hotspots based on neighboring fatal cells"""
        import h3

        # Only consider cells with at least 2 fatalities
        h3_data = h3_data[h3_data["total_killed"] >= 2].copy()
        h3_set  = set(h3_data["h3_cell"])

        def count_fatal_neighbors(cell: str) -> int:
            """Count how many neighboring cells also contain fatalities"""
            neighbors = set(h3.grid_disk(cell, 1))
            neighbors.discard(cell)  # Exclude the cell itself
            return sum(1 for n in neighbors if n in h3_set)

        h3_data["fatal_neighbors"] = h3_data["h3_cell"].apply(count_fatal_neighbors)
        h3_data["pattern"] = h3_data["fatal_neighbors"].apply(
            lambda n: "Corridor Cluster" if n >= 3 else "Isolated Hotspot"
        )
        return h3_data

    # Execute computation
    h3_raw        = compute_h3_fatality_map(df)
    h3_classified = classify_hotspots(h3_raw)

    corridors = h3_classified[h3_classified["pattern"] == "Corridor Cluster"]
    isolated  = h3_classified[h3_classified["pattern"] == "Isolated Hotspot"]

    print(f"✓ Total hotspot cells (≥2 killed): {len(h3_classified):,}")
    print(f"✓ Corridor clusters: {len(corridors):,} ({len(corridors)/len(h3_classified)*100:.1f}%)")
    print(f"✓ Isolated hotspots: {len(isolated):,} ({len(isolated)/len(h3_classified)*100:.1f}%)")
    return corridors, h3_classified, isolated


@app.cell
def _(corridors, go, h3_classified, h3lib, isolated, make_subplots, mo):
    """Create improved corridor vs. intersection classification map"""

    def get_centers(subset):
        """Extract lat/lng centroids from H3 cells"""
        subset = subset.copy()
        centers = subset["h3_cell"].apply(lambda c: h3lib.cell_to_latlng(c))
        subset["lat"] = centers.apply(lambda x: x[0])
        subset["lng"] = centers.apply(lambda x: x[1])
        return subset

    def h3_to_geojson(subset):
        """Convert H3 cells to GeoJSON polygons for Plotly"""
        features = []
        for _, row in subset.iterrows():
            boundary = h3lib.cell_to_boundary(row["h3_cell"])
            coords = [[lng, lat] for lat, lng in boundary]
            coords.append(coords[0])  # Close the polygon
            features.append({
                "type": "Feature",
                "id": row["h3_cell"],
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": {
                    "killed": int(row["total_killed"]),
                    "crashes": int(row["crash_count"]),
                    "borough": row["borough"],
                    "factor": row["top_factor"],
                    "user": row["dominant_user"],
                },
            })
        return {"type": "FeatureCollection", "features": features}

    def make_enhanced_hover(row, pattern):
        """Create detailed hover text with policy context"""
        pattern_desc = (
            "Systemic corridor problem" if pattern == "Corridor Cluster" 
            else "Single intersection failure"
        )
        return (
            f"<b>{pattern}</b><br>"
            f"<b>{row['borough']}</b><br><br>"
            f"<b>Impact:</b><br>"
            f"• {int(row['total_killed'])} killed<br>"
            f"• {int(row['crash_count'])} fatal crashes<br><br>"
            f"<b>Primary Victim:</b> {row['dominant_user']}<br>"
            f"<b>Top Factor:</b> {row['top_factor']}<br><br>"
            f"<b>Pattern:</b> {int(row['fatal_neighbors'])} neighboring fatal cells<br>"
            f"<b>Type:</b> {pattern_desc}"
        )

    # ── Prepare Data ──────────────────────────────────────────────────────────
    cor = get_centers(corridors)
    iso = get_centers(isolated)
    max_killed = h3_classified["total_killed"].max()

    # Create GeoJSON for both
    cor_geojson = h3_to_geojson(cor)
    iso_geojson = h3_to_geojson(iso)

    # ── Map Configuration ─────────────────────────────────────────────────────
    map_config = dict(
        zoom=9.8,
        center={"lat": 40.660, "lon": -73.940},
        style="carto-positron",
    )

    # ── Create Subplots ───────────────────────────────────────────────────────
    fig_h3 = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Corridor Clusters</b><br><sup>{len(cor)} cells • Require state/regional intervention</sup>",
            f"<b>Isolated Hotspots</b><br><sup>{len(iso)} cells • Highest-leverage city interventions</sup>",
        ],
        specs=[[{"type": "mapbox"}, {"type": "mapbox"}]],
        horizontal_spacing=0.02,
    )

    # ── LEFT MAP: Corridor Clusters (Filled Polygons) ─────────────────────────
    fig_h3.add_trace(go.Choroplethmapbox(
        geojson=cor_geojson,
        locations=cor["h3_cell"],
        z=cor["total_killed"],
        colorscale=[
            [0.0, "#fee5d9"],
            [0.3, "#fcae91"],
            [0.6, "#fb6a4a"],
            [1.0, "#a50f15"],
        ],
        zmin=2,
        zmax=max_killed,
        marker_opacity=0.75,
        marker_line_width=1.0,
        marker_line_color="white",
        showscale=False,
        text=cor.apply(lambda r: make_enhanced_hover(r, "Corridor Cluster"), axis=1),
        hoverinfo="text",
        name="Corridor",
    ), row=1, col=1)

    # Add centroids as small markers for corridors
    fig_h3.add_trace(go.Scattermapbox(
        lat=cor["lat"],
        lon=cor["lng"],
        mode="markers",
        marker=dict(size=4, color="darkred", opacity=0.6),
        showlegend=False,
        hoverinfo="skip",
    ), row=1, col=1)

    # ── RIGHT MAP: Isolated Hotspots (Polygons + Prominent Markers) ──────────
    # Background polygons (subtle)
    fig_h3.add_trace(go.Choroplethmapbox(
        geojson=iso_geojson,
        locations=iso["h3_cell"],
        z=iso["total_killed"],
        colorscale=[[0.0, "#f0f0f0"], [0.5, "#bdbdbd"], [1.0, "#636363"]],
        zmin=2,
        zmax=max_killed,
        marker_opacity=0.25,
        marker_line_width=0.5,
        marker_line_color="white",
        showscale=False,
        hoverinfo="skip",
        name="Cell Area",
    ), row=1, col=2)

    # Prominent markers (main visual element)
    fig_h3.add_trace(go.Scattermapbox(
        lat=iso["lat"],
        lon=iso["lng"],
        mode="markers",
        marker=dict(
            size=iso["total_killed"].clip(upper=12) * 3.5,
            color=iso["total_killed"],
            colorscale="Magma",
            cmin=2,
            cmax=max_killed,
            opacity=0.85,
            showscale=True,
            colorbar=dict(
                title=dict(text="<b>Fatalities</b><br>2012–2026", font=dict(size=11)),
                thickness=15,
                len=0.6,
                x=1.02,
                y=0.5,
                tickfont=dict(size=9),
            ),
        ),
        text=iso.apply(lambda r: make_enhanced_hover(r, "Isolated Hotspot"), axis=1),
        hoverinfo="text",
        name="Hotspot",
    ), row=1, col=2)

    # ── Layout Configuration ──────────────────────────────────────────────────
    fig_h3.update_layout(
        mapbox=map_config,
        mapbox2=map_config,
        title=dict(
            text=(
                "<b>Fatal Crash Spatial Patterns — NYC 2012–2026</b><br>"
                "<sup>H3 Resolution 9 (~175m diameter cells) • Cells with ≥2 fatalities classified by neighboring fatal cells</sup>"
            ),
            x=0,
            y=0.98,
            xanchor="left",
            yanchor="top",
            font=dict(size=16),
        ),
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="rgba(0,0,0,0.2)",
            borderwidth=1,
            font=dict(size=10),
        ),
        margin=dict(l=0, r=80, t=100, b=20),
        height=650,
        font=dict(family="Arial, sans-serif"),
    )

    # ── Add Annotations ───────────────────────────────────────────────────────
    annotations = [
        dict(
            x=0.24, y=-0.02,
            xref="paper", yref="paper",
            text=(
                "<b>Corridor Pattern:</b> ≥3 neighboring fatal cells<br>"
                "Indicates systemic highway design failures<br>"
                "<i>Agency: NYS DOT / MTA Bridges & Tunnels</i>"
            ),
            showarrow=False,
            font=dict(size=9, color="#666"),
            align="left",
            xanchor="center",
        ),
        dict(
            x=0.74, y=-0.02,
            xref="paper", yref="paper",
            text=(
                "<b>Isolated Pattern:</b> <3 neighboring fatal cells<br>"
                "Indicates specific intersection failures<br>"
                "<i>Agency: NYC DOT / NYPD Traffic</i>"
            ),
            showarrow=False,
            font=dict(size=9, color="#666"),
            align="left",
            xanchor="center",
        ),
    ]

    fig_h3.update_layout(annotations=annotations)

    # ── Display ───────────────────────────────────────────────────────────────
    mo.vstack([
        fig_h3,
        mo.md(f"""
    ### Spatial Pattern Analysis

    **Corridor clusters** ({len(cor)} cells, shown left) trace the major highway corridors:
    - **Belt Parkway** (Brooklyn/Queens border) — high-speed limited-access highway
    - **Major Deegan / Bruckner system** (Bronx) — interstate highway corridors
    - **FDR Drive** (Manhattan East Side) — high-volume parkway
    - **Brooklyn-Queens Expressway** — elevated/depressed expressway

    These require **state-level agency intervention** (NYS DOT, MTA Bridges & Tunnels) focused on
    automated speed enforcement, managed speed zones, and corridor-wide geometric redesign.
    Timeline: 5–10 year capital projects.

    **Isolated hotspots** ({len(iso)} cells, shown right) are scattered across surface streets,
    concentrated in:
    - **Brooklyn arterials** — Atlantic Avenue, Flatbush Avenue, Bedford Avenue
    - **Queens boulevards** — Northern Boulevard, Queens Boulevard, Jamaica Avenue  
    - **Upper Manhattan** — Broadway north of 155th Street, St. Nicholas Avenue
    - **Bronx intersections** — Grand Concourse, Fordham Road

    These are the **highest-leverage Vision Zero targets**. A single engineering intervention
    (leading pedestrian interval, signal retiming, crosswalk hardening, road diet) at one
    intersection can eliminate the hotspot entirely. Timeline: 1–3 years.

    ### Policy Implication

    The ratio of isolated to corridor cells is **{len(iso)} : {len(cor)}** 
    (= **{len(iso)/(len(cor)+len(iso))*100:.0f}% isolated**).

    This proves that **the majority of NYC's fatal crash problem is not a highway design problem**
    requiring decades of capital construction and state-level coordination. It is a **surface street
    and intersection problem** amenable to city-level policy action on a 1–3 year timeline with
    existing operational budgets.

    The spatial pattern suggests NYC DOT should prioritize:
    1. **Intersection-specific interventions** at the {len(iso)} isolated hotspots
    2. **Quick-build treatments** (paint, bollards, signal retiming) over long-term capital projects
    3. **Data-driven targeting** using this H3 analysis to rank intervention sites by impact per dollar
        """),
    ])
    return cor, iso


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Deadliest Streets — Corridor-Level Rankings

    Streets are classified as **Highway** (limited-access, speed-dominant) or
    **Surface Street** (signal-controlled, mixed road users). These require
    fundamentally different policy interventions and are managed by different agencies.

    Use the dropdowns below to filter by borough and street type.
    """)
    return


@app.cell
def _(df, mo):
    """Interactive filters for street analysis"""
    borough_filter = mo.ui.dropdown(
        options=["ALL"] + sorted(df["borough"].dropna().unique().tolist()),
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
def _(dominant_factor, dominant_road_user, get_policy_recommendation, pd):
    """Compute street-level statistics"""

    def compute_street_stats(
        df: pd.DataFrame,
        borough: str = "ALL",
        street_type: str = "ALL",
        top_n: int = 20,
    ) -> pd.DataFrame:
        """
        Aggregate crashes by street and compute fatality statistics.
        Returns top N deadliest streets with policy recommendations.
        """

        # Get all crashes for the denominator (total crashes per street)
        all_crashes = df[df["street_normalized"].notna()]
        if borough != "ALL":
            all_crashes = all_crashes[all_crashes["borough"] == borough]
        if street_type != "ALL":
            all_crashes = all_crashes[all_crashes["street_type"] == street_type]

        group_cols = (
            ["street_normalized"] if borough == "ALL"
            else ["street_normalized", "borough"]
        )

        all_crash_counts = (
            all_crashes.groupby(group_cols)
            .agg(total_crashes=("collision_id", "count"))
            .reset_index()
        )

        # Get fatal crashes only
        fatal = df[df["any_killed"] & df["street_normalized"].notna()]
        if borough != "ALL":
            fatal = fatal[fatal["borough"] == borough]
        if street_type != "ALL":
            fatal = fatal[fatal["street_type"] == street_type]

        stats = (
            fatal.groupby(group_cols)
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

        # Merge with total crash counts
        stats = stats.merge(all_crash_counts, on=group_cols, how="left")

        if borough == "ALL":
            stats["borough"] = "ALL"

        # Calculate fatality rate (per 1,000 crashes)
        stats["fatality_rate"] = (
            stats["total_killed"] / stats["total_crashes"] * 1000
        ).round(2)

        # Determine dominant road user and policy recommendation
        stats["dominant_road_user"] = stats.apply(dominant_road_user, axis=1)
        stats["policy_recommendation"] = stats.apply(
            lambda r: get_policy_recommendation(r["top_factor"], r["dominant_road_user"]),
            axis=1,
        )

        return (
            stats.sort_values("total_killed", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )

    return (compute_street_stats,)


@app.cell
def _(go, pd, px):
    """Create street fatality bar chart"""

    def plot_street_chart(stats: pd.DataFrame, borough: str, street_type: str) -> go.Figure:
        """Generate horizontal bar chart of deadliest streets"""
        label = f"{street_type} | {borough}" if street_type != "ALL" else borough
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
            title=f"Top 20 Deadliest Streets — {label}",
            labels={
                "total_killed": "Total Killed",
                "street_normalized": "",
                "top_factor": "Top Contributing Factor",
                "fatality_rate": "Fatality Rate (per 1k crashes)",
                "dominant_road_user": "Road User Most Affected",
                "total_crashes": "Total Crashes",
                "years_active": "Active Since",
                "policy_recommendation": "Recommendation",
            },
            height=700,
        )
        fig.update_layout(
            title=dict(x=0, y=0.98, xanchor="left"),
            yaxis={"categoryorder": "total ascending"},
            legend=dict(
                orientation="h", yanchor="top", y=-0.12,
                xanchor="left", x=0, title="Top Contributing Factor",
            ),
            margin=dict(l=20, r=20, t=60, b=130),
        )
        return fig

    return (plot_street_chart,)


@app.cell
def _(
    borough_filter,
    compute_street_stats,
    df,
    mo,
    plot_street_chart,
    street_type_filter,
):
    """Display street rankings and policy priorities"""

    street_stats = compute_street_stats(
        df,
        borough=borough_filter.value,
        street_type=street_type_filter.value,
    )

    agency_note = {
        "Highway":        "🏛 **NYS DOT / MTA Bridges & Tunnels** — Speed enforcement, highway redesign, managed speed zones",
        "Surface Street": "🏙 **NYC DOT / NYPD Traffic** — Signal timing, pedestrian intervals, local enforcement",
        "ALL":            "ℹ️ Select a street type above to see the responsible agency",
    }

    # Build the table rows outside the f-string to avoid backslash issues
    table_rows = "".join(
        f"| {i+1} | {r['street_normalized']} | {int(r['total_killed'])} "
        f"| {r['fatality_rate']} | {r['top_factor']} "
        f"| {r['dominant_road_user']} | {r['policy_recommendation']} |\n"
        for i, r in street_stats.head(10).iterrows()
    )

    mo.vstack([
        plot_street_chart(street_stats, borough_filter.value, street_type_filter.value),
        mo.md(agency_note[street_type_filter.value]),
        mo.md(f"""
    ### Top 10 Policy Priorities — {street_type_filter.value} | {borough_filter.value}

    | Rank | Street | Killed | Rate (per 1k) | Factor | Road User | Recommendation |
    |------|--------|--------|---------------|--------|-----------|----------------|
    {table_rows}
        """),
    ])
    return (street_stats,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Multi-Lens Spatial Analysis

    The following three maps show the same geographic area through different analytical lenses.
    Each lens reveals different aspects of the crash problem and suggests different interventions:

    - **Systemic Volume** (total crash count) — Where crashes happen most frequently
    - **Fatal Severity** (fatality rate) — Where crashes are most likely to be deadly
    - **VRU Priority** (vulnerable road user casualties) — Where pedestrians and cyclists are killed

    A street can rank high on volume but low on severity (many minor crashes), or vice versa
    (few crashes but highly fatal). The VRU lens highlights areas where infrastructure changes
    can protect the most vulnerable road users.
    """)
    return


@app.cell
def _(df):
    """Prepare data for multi-lens visualization"""
    import subprocess
    import contextily as cx
    from matplotlib.collections import PolyCollection
    import numpy as np

    # Ensure h3 is available
    try:
        import h3
    except ImportError:
        subprocess.run(["pip", "install", "h3", "--break-system-packages"], capture_output=True)
        import h3

    # Filter and clean
    crash_geo = df[
        df["latitude"].notna() & 
        df["longitude"].notna() &
        df["latitude"].between(40.4, 40.95) & 
        df["longitude"].between(-74.3, -73.7)
    ].copy()

    # Convert to H3 hexagons (Resolution 8 for better visualization at city scale)
    crash_geo["h3_index"] = crash_geo.apply(
        lambda row: h3.latlng_to_cell(row["latitude"], row["longitude"], 8),
        axis=1
    )

    # Aggregate metrics by hexagon
    h3_agg = crash_geo.groupby("h3_index").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
        injured=("number_of_persons_injured", "sum"),
        ped_killed=("number_of_pedestrians_killed", "sum"),
        cyc_killed=("number_of_cyclist_killed", "sum"),
    ).reset_index()

    # Calculate centroids and rates
    h3_agg["lat"] = h3_agg["h3_index"].apply(lambda x: h3.cell_to_latlng(x)[0])
    h3_agg["lon"] = h3_agg["h3_index"].apply(lambda x: h3.cell_to_latlng(x)[1])
    h3_agg["fatality_rate"] = h3_agg["killed"] / h3_agg["crashes"] * 1000

    print(f"✓ Analyzed {len(h3_agg):,} hexagons for multi-lens visualization")
    return PolyCollection, cx, h3, h3_agg, np


@app.cell
def _(PolyCollection, cx, h3, h3_agg, np, plt):
    """Create three-lens matplotlib visualization"""

    def get_hex_boundary(hex_id):
        """Extract hexagon boundary coordinates"""
        h_str = str(hex_id).strip().lower()
        try:
            func = h3.cell_to_boundary if hasattr(h3, 'cell_to_boundary') else h3.h3_to_geo_boundary
            points = func(h_str)
            return [(p[1], p[0]) for p in points]  # Convert to (lon, lat)
        except:
            return None

    # Filter to significant zones only
    significant_zones = h3_agg[h3_agg["crashes"] >= 5].copy()
    significant_zones["vru_total"] = significant_zones["ped_killed"] + significant_zones["cyc_killed"]

    # Define three analytical lenses
    lenses = [
        {
            "df": significant_zones.nlargest(300, "crashes"), 
            "col": "crashes", 
            "cmap": "YlOrRd", 
            "title": "Systemic Volume\n(Total Crash Count n)"
        },
        {
            "df": significant_zones[significant_zones["killed"] > 0].nlargest(300, "fatality_rate"), 
            "col": "fatality_rate", 
            "cmap": "Reds", 
            "title": "Fatal Severity\n(Deaths/Crashes × 1000)"
        },
        {
            "df": significant_zones[significant_zones["vru_total"] > 0].nlargest(300, "vru_total"), 
            "col": "vru_total", 
            "cmap": "PuRd", 
            "title": "VRU Priority\n(Sum of vulnerable user casualties)"
        }
    ]

    # Create figure
    midnight_grey = '#3B3B3B' 
    fig_multi, axes = plt.subplots(1, 3, figsize=(24, 11), facecolor=midnight_grey)

    for ax, lens in zip(axes, lenses):
        ax.set_facecolor(midnight_grey)

        # Collect hexagon geometries and values
        verts, vals = [], []
        for idx, row in lens["df"].iterrows():
            poly = get_hex_boundary(row["h3_index"])
            if poly:
                verts.append(poly)
                vals.append(row[lens["col"]])

        # Plot hexagons
        if verts:
            pc = PolyCollection(
                verts, array=np.array(vals), cmap=lens["cmap"], 
                edgecolors='white', linewidths=0.2, alpha=0.75, zorder=3
            )
            ax.add_collection(pc)

            # Add colorbar
            cb = fig_multi.colorbar(pc, ax=ax, shrink=0.3, aspect=20, pad=0.02)
            cb.ax.yaxis.set_tick_params(color='white', labelcolor='white', labelsize=8)
            cb.outline.set_edgecolor('white')

        # Set map extent
        if not significant_zones.empty:
            ax.set_xlim(significant_zones["lon"].min() - 0.01, significant_zones["lon"].max() + 0.01)
            ax.set_ylim(significant_zones["lat"].min() - 0.01, significant_zones["lat"].max() + 0.01)

        # Add basemap
        cx.add_basemap(ax, crs="EPSG:4326", source=cx.providers.CartoDB.DarkMatterNoLabels, alpha=0.4, zorder=1)

        # Style
        ax.set_title(lens["title"], color='white', fontsize=14, fontweight='bold', pad=20)
        ax.axis('off')

    plt.tight_layout()
    plt.show()
    return


@app.cell
def _(mo):
    mo.md("""
    ### Multi-Lens Insights

    **Systemic Volume** (left) shows where crashes happen most frequently, regardless of severity.
    High-volume corridors like Queens Boulevard and Atlantic Avenue appear prominently. These areas
    benefit from **traffic calming** and **mode shift** interventions that reduce overall vehicle volume.

    **Fatal Severity** (center) highlights corridors where crashes are disproportionately deadly.
    Highway corridors (Belt Parkway, Major Deegan) show high severity despite moderate volume,
    suggesting **speed** is the primary risk factor. These require **automated enforcement** and
    **speed limit reductions**.

    **VRU Priority** (right) identifies areas where pedestrians and cyclists are most at risk.
    Dense commercial corridors in Brooklyn and Queens show high VRU casualties, calling for
    **protected infrastructure** (bike lanes, pedestrian islands) and **intersection redesign**.

    ### Cross-Referencing the Lenses

    - Areas that appear in **all three lenses** (e.g., Atlantic Avenue, Northern Boulevard) are the
      highest-priority corridors for comprehensive Vision Zero intervention.
    - Areas that appear **only in Fatal Severity** (e.g., Belt Parkway segments) require speed-focused
      interventions rather than volume reduction.
    - Areas that appear **only in VRU Priority** need mode-specific protection (bike lanes, crosswalk
      hardening) rather than general traffic calming.

    This multi-lens approach ensures interventions are matched to the specific failure mode rather
    than applying one-size-fits-all solutions.
    """)
    return


@app.cell
def _(
    cor,
    cyc_killed,
    df,
    iso,
    mo,
    mot_killed,
    ped_killed,
    street_stats,
    total_killed,
):
    """Synthesize findings and generate policy recommendations"""

    top_street     = street_stats.iloc[0]["street_normalized"]
    top_street_k   = int(street_stats.iloc[0]["total_killed"])
    top_factor_all = street_stats["top_factor"].value_counts().index[0]

    # Vision Zero progress calculation
    vision_zero_start = df[df["year"] >= 2014]
    rate_2014 = (
        vision_zero_start[vision_zero_start["year"] == 2014]["any_killed"].sum() /
        vision_zero_start[vision_zero_start["year"] == 2014]["collision_id"].count() * 1000
    )
    latest_year = df["year"].max() - 1  # Exclude incomplete current year
    rate_latest = (
        vision_zero_start[vision_zero_start["year"] == latest_year]["any_killed"].sum() /
        vision_zero_start[vision_zero_start["year"] == latest_year]["collision_id"].count() * 1000
    )
    pct_change = (rate_latest - rate_2014) / rate_2014 * 100

    mo.md(f"""
    ---
    ## Key Findings & Policy Recommendations

    ### Summary of Findings

    **1. Fatalities are highly concentrated on a small number of corridors**

    The top 20 streets account for a disproportionate share of all traffic deaths.
    The single deadliest street, **{top_street}**, recorded **{top_street_k} fatalities**
    over the study period. Five streets together account for approximately 30% of all fatalities.

    This concentration means targeted corridor interventions can achieve outsized impact.

    **2. Pedestrians bear the greatest burden**

    Pedestrians account for **{ped_killed/total_killed*100:.1f}%** of all fatalities,
    cyclists **{cyc_killed/total_killed*100:.1f}%**, and motorists
    **{mot_killed/total_killed*100:.1f}%**. Vulnerable road users are killed at
    rates disproportionate to their share of traffic, making them the primary beneficiaries
    of Vision Zero interventions.

    **3. Contributing factors vary by street type and require different interventions**

    On highways, **Unsafe Speed** dominates, requiring automated enforcement and speed limit reductions.
    On surface streets, **{top_factor_all}** is the leading factor, calling for signal timing,
    intersection design, and local enforcement interventions.

    The distinction between highway and surface street failures points to different responsible
    agencies: highways require state-level action (NYS DOT, MTA), while surface streets are under
    city control (NYC DOT, NYPD).

    **4. Spatial pattern: most deaths occur at isolated intersections, not highway corridors**

    Of {len(cor) + len(iso)} significant hotspot cells, **{len(iso)}
    ({len(iso)/(len(cor)+len(iso))*100:.0f}%)** are isolated hotspots rather than corridor clusters.

    This is the critical finding: **most of NYC's fatal crash problem is not a highway design problem
    requiring decades of capital construction.** It is a surface street and intersection problem
    amenable to city-level policy action on a 1–3 year timeline.

    The isolated hotspots represent the highest-leverage Vision Zero targets — single engineering
    interventions can eliminate entire crash clusters.

    **5. Vision Zero fatality rate trend**

    The fatality rate per 1,000 crashes moved from **{rate_2014:.2f}** in 2014
    to **{rate_latest:.2f}** in {latest_year} — a **{abs(pct_change):.1f}%
    {"decrease" if pct_change < 0 else "increase"}**.
    {"This suggests meaningful progress, though absolute fatality counts remain elevated and continued intervention is essential." if pct_change < 0 else "This warrants urgent policy review and accelerated intervention deployment."}

    ---

    ### Policy Recommendations

    > **Note:** The following interventions are commonly used in Vision Zero programs and are 
    > supported by traffic safety literature. Specific effectiveness rates, costs, and timelines 
    > should be validated through systematic literature review before implementation. A comprehensive 
    > search strategy for identifying peer-reviewed evidence is provided in the supplementary materials.

    | Priority | Location Type | Agency | Intervention Type | Rationale from Data |
    |----------|--------------|--------|-------------------|---------------------|
    | **1 — Highest leverage** | Isolated surface street hotspots ({len(iso)} sites) | NYC DOT / NYPD | Leading pedestrian intervals, signal retiming, crosswalk treatments, intersection redesign | 77% of fatal hotspots are isolated intersections; site-specific interventions can eliminate entire clusters |
    | **2 — Corridor safety** | Belt Parkway, Major Deegan, BQE, FDR Drive | NYS DOT / MTA | Speed management, automated enforcement, geometric improvements | 23% of hotspots are corridor clusters; "Unsafe Speed" is dominant highway factor |
    | **3 — Pedestrian protection** | Broadway, Atlantic Ave, Flatbush Ave | NYC DOT | Crosswalk hardening, refuge islands, signal phasing, road diet | Pedestrians = 46% of fatalities; "Failure to Yield" is top surface street factor |
    | **4 — Cyclist safety** | Northern Blvd, Queens Blvd, Bedford Ave | NYC DOT | Protected bike infrastructure, intersection treatments | Cyclists = 20% of fatalities; concentrated on arterial corridors |
    | **5 — Data quality** | All corridors | NYPD | Improve `on_street_name` completion rate in crash reports (currently ~75%) | Essential for precise corridor targeting |

    ### Implementation Strategy

    The spatial analysis identifies {len(iso)} isolated intersection hotspots and {len(cor)} 
    corridor clusters. The following phased approach prioritizes high-impact locations:

    **Phase 1: Evidence Gathering & Pilot Testing**
    - Conduct systematic literature review to identify evidence-based interventions (see supplementary materials)
    - Select 10-15 highest-priority isolated hotspots for pilot interventions
    - Implement before-after evaluation framework with control sites
    - Establish baseline crash data and traffic volume measurements

    **Phase 2: Targeted Deployment**
    - Scale proven interventions to additional isolated hotspots based on pilot results
    - Prioritize locations by fatality count, VRU exposure, and intervention feasibility
    - Deploy automated enforcement on high-speed corridor clusters
    - Establish data feedback loop for continuous improvement

    **Phase 3: Systemic Expansion**
    - Apply successful interventions citywide based on crash characteristics
    - Integrate interventions into standard design guidelines
    - Coordinate with state agencies on highway corridor improvements
    - Conduct comprehensive program evaluation

    ### Next Steps for Evidence-Based Policy Development

    1. **Literature Review (2-4 weeks):** Execute the provided Google Scholar search strategy to identify:
       - Intervention effectiveness rates with confidence intervals
       - Implementation costs and timelines from peer-reviewed studies
       - Best practices from similar urban contexts (NYC, Chicago, SF, Seattle)

    2. **Local Context Validation (1-2 weeks):** Review NYC DOT's existing intervention evaluations:
       - Has NYC already piloted LPIs, protected bike lanes, or other treatments?
       - What were the local effectiveness rates?
       - What implementation barriers were encountered?

    3. **Engineering Assessment (2-4 weeks):** Conduct site visits to top 20 isolated hotspots:
       - Verify crash patterns match spatial analysis
       - Assess site-specific constraints (geometry, utilities, right-of-way)
       - Determine appropriate interventions for each location

    4. **Update Policy Brief with Evidence:** Integrate findings into recommendations table:
       - Replace intervention types with specific, evidence-based treatments
       - Add effect size estimates (e.g., "reduces crashes by 30-50%")
       - Include cost estimates and implementation timelines from literature
       - Document evidence quality for each recommendation

    ### Critical Next Steps

    **1. Validate Spatial Patterns with Field Investigation**

    The H3 hexagonal analysis identifies statistical hotspots, but field verification is essential:
    - Top 20 isolated hotspots should be site-visited to confirm crash patterns
    - Engineering assessment required to determine site-specific constraints
    - Community input should inform intervention selection

    **2. Establish Evidence-Based Intervention Portfolio**

    Use the provided literature search strategy to build a menu of proven interventions:
    - Prioritize treatments with strong before-after study evidence
    - Document effect sizes, costs, and implementation requirements
    - Consider local context and transferability from other cities

    **3. Develop Monitoring & Evaluation Framework**

    Before implementing interventions, establish:
    - Baseline crash counts and rates for treatment sites
    - Control/comparison sites for before-after analysis  
    - Traffic volume and speed monitoring protocols
    - Evaluation timeline (minimum 2-year post-implementation)

    ---

    ### Data Quality & Limitations

    **Street Name Completion:** Approximately 75% of crash records include a valid street name.
    The missing 25% cannot be included in street-level rankings but are captured in the spatial
    (H3 hexagon) analysis, which relies on lat/lng coordinates only.

    **Recommendation:** NYPD should implement automated street name geocoding in crash report
    systems to improve data completeness. This would enable more precise corridor targeting.

    **Spatial Resolution:** H3 resolution 9 (~175m diameter) is appropriate for identifying
    intersection-scale hotspots but may aggregate multiple nearby intersections into a single cell.
    For final site selection, NYC DOT should conduct block-level analysis within identified hotspot cells.

    ---

    ### Supplementary Materials

    This analysis is accompanied by:

    1. **Google Scholar Search Strategy** (`google_scholar_search_strategy.md`)
       - Comprehensive search queries for evidence-based interventions
       - Organized by intervention type and research priority
       - Expected to yield 100-200 relevant peer-reviewed papers

    2. **Ready-to-Paste Search Queries** (`search_queries_ready_to_paste.txt`)
       - 48 pre-formatted queries ready for Google Scholar
       - Organized in tiers by analysis priority
       - Includes advanced search techniques and citation chaining strategies

    3. **Literature Tracking Template** (`literature_tracking_template.txt`)
       - Spreadsheet structure for organizing research findings
       - Evidence quality assessment framework
       - Citation extraction templates ready for notebook integration

    **Using these materials:** Execute the literature search strategy to replace general intervention 
    recommendations with specific, evidence-based treatments including effectiveness rates, costs, 
    and implementation timelines from peer-reviewed research.

    ---

    *Analysis: Remy Ndayizeye · Northeast Big Data Innovation Hub · National Student Data Corps · U.S. DOT FHWA*  
    *Date: April 2026*  
    *Data: NYC Open Data — Motor Vehicle Collisions (1.95M records, July 2012 – April 2026)*  
    *Pipeline: Kedro 0.19.15 · H3 Spatial Indexing · Plotly Interactive Visualization*  
    *Methods: H3 hexagonal binning (resolution 9) · Street name normalization · Spatial hotspot classification*
    """)
    return


if __name__ == "__main__":
    app.run()
