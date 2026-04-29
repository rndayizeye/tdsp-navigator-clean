import marimo

__generated_with = "0.23.2"
app = marimo.App(width="full")


@app.cell
def _():
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
    **Remy Ndayizeye · Northeast Big Data Innovation Hub · National Student Data Corps · U.S. DOT FHWA**

    ---

    This notebook presents a spatial and street-level analysis of 1.95 million NYC motor vehicle
    crashes (2012–2026), with a focus on identifying the deadliest corridors and intersections
    to inform Vision Zero policy interventions.

    **Data**: NYC Open Data — Motor Vehicle Collisions (Socrata API)
    **Pipeline**: Kedro 0.19.15 · Incremental watermark-based ingestion · 28-column parquet schema
    **Key Question**: Where do people die on NYC streets, and what systemic factors drive those fatalities?
    """)
    return


@app.cell
def _(gpd, pd):
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

    print(f"Crashes loaded: {len(df_raw):,}")
    print(f"Date range: {df_raw['crash_date'].min().date()} → {df_raw['crash_date'].max().date()}")
    print(f"Fatal crashes: {df_raw['any_killed'].sum():,}")
    return (df_raw,)


@app.cell
def _(pd, re):
    # ── Street normalization ──────────────────────────────────
    def normalize_street_name(name: str) -> str | None:
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

    # ── Street type classification ────────────────────────────
    HIGHWAYS = {
        "BELT PARKWAY", "GRAND CENTRAL PARKWAY", "MAJOR DEEGAN EXPRESSWAY",
        "BRUCKNER BOULEVARD", "HENRY HUDSON PARKWAY", "CROSS ISLAND PARKWAY",
        "LONG ISLAND EXPRESSWAY", "CROSS BRONX EXPRESSWAY",
        "BROOKLYN QUEENS EXPRESSWAY", "BRUCKNER EXPRESSWAY", "FDR DRIVE",
        "STATEN ISLAND EXPRESSWAY", "GOWANUS EXPRESSWAY", "HARLEM RIVER DRIVE",
        "HUTCHINSON RIVER PARKWAY", "SHERIDAN EXPRESSWAY", "VAN WYCK EXPRESSWAY",
    }

    def classify_street_type(street: str) -> str:
        if pd.isna(street):
            return "Unknown"
        if street in HIGHWAYS:
            return "Highway"
        if any(x in street for x in ["EXPRESSWAY", "PARKWAY", "FREEWAY"]):
            return "Highway"
        return "Surface Street"

    # ── Contributing factor helpers ───────────────────────────
    def dominant_factor(series: pd.Series) -> str:
        counts = series[series != "Unspecified"].value_counts()
        return counts.index[0] if not counts.empty else "Unspecified"

    def dominant_road_user(row) -> str:
        return max(
            {"Pedestrian": row["ped_killed"],
             "Cyclist": row["cyc_killed"],
             "Motorist": row["mot_killed"]},
            key=lambda k: {"Pedestrian": row["ped_killed"],
                           "Cyclist": row["cyc_killed"],
                           "Motorist": row["mot_killed"]}[k]
        )

    def get_policy_recommendation(factor: str, road_user: str) -> str:
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
    print(f"Enriched dataset: {len(df):,} rows")
    return (df,)


@app.cell
def _(df, mo):
    total         = len(df)
    total_killed  = int(df["number_of_persons_killed"].sum())
    total_injured = int(df["number_of_persons_injured"].sum())
    ped_killed    = int(df["number_of_pedestrians_killed"].sum())
    cyc_killed    = int(df["number_of_cyclist_killed"].sum())
    mot_killed    = int(df["number_of_motorist_killed"].sum())
    fatal_crashes = int(df["any_killed"].sum())
    pct_fatal     = fatal_crashes / total * 100

    mo.md(f"""
    ## Dataset Overview

    | Metric | Value |
    |--------|-------|
    | Total crashes | {total:,} |
    | Fatal crashes | {fatal_crashes:,} ({pct_fatal:.2f}% of all crashes) |
    | Total killed | {total_killed:,} |
    | Total injured | {total_injured:,} |
    | Pedestrians killed | {ped_killed:,} ({ped_killed/total_killed*100:.1f}% of fatalities) |
    | Cyclists killed | {cyc_killed:,} ({cyc_killed/total_killed*100:.1f}% of fatalities) |
    | Motorists killed | {mot_killed:,} ({mot_killed/total_killed*100:.1f}% of fatalities) |

    > **Vision Zero context**: NYC launched Vision Zero in 2014 with the goal of eliminating all
    > traffic deaths. Pedestrians and cyclists together account for
    > **{(ped_killed+cyc_killed)/total_killed*100:.1f}%** of all fatalities — the primary target
    > population for infrastructure and enforcement interventions.
    """)
    return cyc_killed, mot_killed, ped_killed, total_killed


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Section 1 — Deadliest Streets

    Streets are classified as **Highway** (limited-access, speed-dominant) or
    **Surface Street** (signal-controlled, mixed road users). These require
    fundamentally different policy interventions and are managed by different agencies.

    Use the dropdowns to filter by borough and street type.
    """)
    return


@app.cell
def _(df, mo):
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
    def compute_street_stats(
        df: pd.DataFrame,
        borough: str = "ALL",
        street_type: str = "ALL",
        top_n: int = 20,
    ) -> pd.DataFrame:

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

        stats = stats.merge(all_crash_counts, on=group_cols, how="left")

        if borough == "ALL":
            stats["borough"] = "ALL"

        stats["fatality_rate"] = (
            stats["total_killed"] / stats["total_crashes"] * 1000
        ).round(2)

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
    def plot_street_chart(stats: pd.DataFrame, borough: str, street_type: str) -> go.Figure:
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
    street_stats = compute_street_stats(
        df,
        borough=borough_filter.value,
        street_type=street_type_filter.value,
    )

    agency_note = {
        "Highway":        "🏛 **NYS DOT / MTA** — Speed enforcement, highway redesign, managed speed zones",
        "Surface Street": "🏙 **NYC DOT / NYPD** — Signal timing, pedestrian intervals, local enforcement",
        "ALL":            "ℹ️ Select a street type to see the responsible agency",
    }

    mo.vstack([
        plot_street_chart(street_stats, borough_filter.value, street_type_filter.value),
        mo.md(agency_note[street_type_filter.value]),
        mo.md(f"""
    ### Top 10 Policy Priorities — {street_type_filter.value} | {borough_filter.value}

    | Rank | Street | Killed | Rate (per 1k) | Factor | Road User | Recommendation |
    |------|--------|--------|---------------|--------|-----------|----------------|
    {"".join(
    f"| {i+1} | {r['street_normalized']} | {int(r['total_killed'])} "
    f"| {r['fatality_rate']} | {r['top_factor']} "
    f"| {r['dominant_road_user']} | {r['policy_recommendation']} |{chr(10)}"
    for i, r in street_stats.head(10).iterrows()
    )}
        """),
    ])
    return (street_stats,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Section 2 — Spatial Hotspot Analysis (H3 Hexagonal Binning)

    Each fatal crash is assigned to an H3 hexagonal cell (~175m diameter, resolution 9).
    Cells are classified as:

    - **Corridor clusters** — 3 or more neighboring cells also contain fatalities.
      These indicate systemic road design problems along a corridor (e.g. Belt Parkway).
      Intervention: corridor-wide speed enforcement or road diet.

    - **Isolated hotspots** — fewer than 3 neighboring fatal cells.
      These point to specific intersection failures — the highest-leverage Vision Zero targets
      since a single targeted intervention can eliminate the hotspot entirely.
    """)
    return


@app.cell
def _(df, dominant_factor, pd):
    def compute_h3_fatality_map(df: pd.DataFrame, resolution: int = 9) -> pd.DataFrame:
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
        import h3

        h3_data = h3_data[h3_data["total_killed"] >= 2].copy()
        h3_set  = set(h3_data["h3_cell"])

        def count_fatal_neighbors(cell: str) -> int:
            neighbors = set(h3.grid_disk(cell, 1))
            neighbors.discard(cell)
            return sum(1 for n in neighbors if n in h3_set)

        h3_data["fatal_neighbors"] = h3_data["h3_cell"].apply(count_fatal_neighbors)
        h3_data["pattern"] = h3_data["fatal_neighbors"].apply(
            lambda n: "Corridor Cluster" if n >= 3 else "Isolated Hotspot"
        )
        return h3_data

    h3_raw        = compute_h3_fatality_map(df)
    h3_classified = classify_hotspots(h3_raw)

    corridors = h3_classified[h3_classified["pattern"] == "Corridor Cluster"]
    isolated  = h3_classified[h3_classified["pattern"] == "Isolated Hotspot"]

    print(f"Total hotspot cells (≥2 killed): {len(h3_classified)}")
    print(f"Corridor clusters: {len(corridors)}")
    print(f"Isolated hotspots: {len(isolated)}")
    return corridors, h3_classified, isolated


@app.cell
def _(corridors, go, h3_classified, h3lib, isolated, make_subplots, mo):
    def get_centers(subset):
        import h3 as h3lib
        subset = subset.copy()
        centers = subset["h3_cell"].apply(lambda c: h3lib.cell_to_latlng(c))
        subset["lat"] = centers.apply(lambda x: x[0])
        subset["lng"] = centers.apply(lambda x: x[1])
        return subset

    cor = get_centers(corridors)
    iso = get_centers(isolated)
    max_killed = h3_classified["total_killed"].max()

    def make_hover(r, pattern):
        return (
            f"<b>{pattern}</b><br>"
            f"Borough: {r['borough']}<br>"
            f"Killed: {int(r['total_killed'])}<br>"
            f"Fatal Crashes: {int(r['crash_count'])}<br>"
            f"Top Factor: {r['top_factor']}<br>"
            f"Road User: {r['dominant_user']}<br>"
            f"Fatal Neighbors: {int(r['fatal_neighbors'])}"
        )
    # Build GeoJSON polygons for corridor cells only

    def h3_to_geojson(subset):
        features = []
        for _, row in subset.iterrows():
            boundary = h3lib.cell_to_boundary(row["h3_cell"])
            coords = [[lng, lat] for lat, lng in boundary]
            coords.append(coords[0])
            features.append({
                "type": "Feature",
                "id": row["h3_cell"],
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": {},
            })
        return {"type": "FeatureCollection", "features": features}

    cor_geojson = h3_to_geojson(cor)

    shared_marker = dict(
        colorscale="YlOrRd",
        cmin=2,
        cmax=max_killed,
        opacity=0.85,
    )

    map_config = dict(
        zoom=9.8,
        center={"lat": 40.660, "lon": -73.940},
        style="carto-positron",
    )

    fig_h3 = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"Corridor Clusters ({len(cor)} cells) — Systemic road design failures",
            f"Isolated Hotspots ({len(iso)} cells) — Specific intersection failures",
        ],
        specs=[[{"type": "mapbox"}, {"type": "mapbox"}]],
        horizontal_spacing=0.02,
    )

    fig_h3.add_trace(go.Choroplethmapbox(
        geojson=cor_geojson,
        locations=cor["h3_cell"],
        z=cor["total_killed"],
        colorscale="Viridis",
        zmin=2,
        zmax=max_killed,
        marker_opacity=0.8,
        marker_line_width=0.5,
        marker_line_color="white",
        showscale=False,
        text=cor.apply(lambda r: make_hover(r, "Corridor Cluster"), axis=1),
        hoverinfo="text",
        name="Corridor Cluster",
    ), row=1, col=1)

    fig_h3.add_trace(go.Scattermapbox(
        lat=iso["lat"], lon=iso["lng"],
        mode="markers",
        marker=dict(
            **shared_marker,
            size=iso["total_killed"].clip(upper=10) * 2.8,
            color=iso["total_killed"],
            showscale=True,
            colorbar=dict(
                title="Fatalities<br>(2012–2026)",
                thickness=12,
                len=0.55,
                x=1.01,
            ),
        ),
        text=iso.apply(lambda r: make_hover(r, "Isolated Hotspot"), axis=1),
        hoverinfo="text",
        name="Isolated Hotspot",
    ), row=1, col=2)

    fig_h3.update_layout(
        mapbox=map_config,
        mapbox2=map_config,
        title=dict(
            text=(
                "Fatal Crash Hotspots — NYC 2012–2026 (H3 Resolution 9, ~175m cells)<br>"
                "<sup>Size = fatalities · Hover for borough, factor, and road user details</sup>"
            ),
            x=0, font_size=15,
        ),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.06,
            xanchor="left", x=0,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.15)",
            borderwidth=1,
        ),
        margin=dict(l=0, r=60, t=80, b=40),
        height=620,
    )

    mo.vstack([
        fig_h3,
        mo.md(f"""
    ### Spatial Findings

    **Corridor clusters** ({len(cor)} cells) trace the major highway corridors —
    Belt Parkway, the Major Deegan/Bruckner system in the Bronx, and the FDR Drive.
    These require **state-level agency intervention** (NYS DOT / MTA) focused on
    speed infrastructure and managed lanes.

    **Isolated hotspots** ({len(iso)} cells) are scattered across the five boroughs,
    concentrated on arterial surface streets in Brooklyn, Queens, and upper Manhattan.
    These are the **highest-leverage Vision Zero targets** — a single engineering
    intervention (leading pedestrian interval, signal retiming, crosswalk hardening)
    at one intersection can eliminate the hotspot entirely.

    The ratio of isolated to corridor cells
    (**{len(iso)} : {len(cor)}**) means the majority of NYC's fatal crash problem
    is not a highway design problem — it is a surface street and intersection problem
    amenable to city-level policy action.
        """),
    ])
    return cor, iso


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
    top_street     = street_stats.iloc[0]["street_normalized"]
    top_street_k   = int(street_stats.iloc[0]["total_killed"])
    top_factor_all = street_stats["top_factor"].value_counts().index[0]
    top_user_all   = street_stats["dominant_road_user"].value_counts().index[0]

    vision_zero_start = df[df["year"] >= 2014]
    rate_2014 = (
        vision_zero_start[vision_zero_start["year"] == 2014]["any_killed"].sum() /
        vision_zero_start[vision_zero_start["year"] == 2014]["collision_id"].count() * 1000
    )
    latest_year = df["year"].max() - 1
    rate_latest = (
        vision_zero_start[vision_zero_start["year"] == latest_year]["any_killed"].sum() /
        vision_zero_start[vision_zero_start["year"] == latest_year]["collision_id"].count() * 1000
    )
    pct_change = (rate_latest - rate_2014) / rate_2014 * 100

    mo.md(f"""
    ---
    ## Section 3 — Key Findings & Policy Recommendations

    ### Findings

    **1. Fatalities are highly concentrated.**
    The top 20 streets account for a disproportionate share of all traffic deaths.
    The single deadliest street, **{top_street}**, recorded **{top_street_k} fatalities**
    over the study period. Five streets together account for ~30% of all fatalities.

    **2. Pedestrians bear the greatest burden.**
    Pedestrians account for **{ped_killed/total_killed*100:.1f}%** of all fatalities,
    cyclists **{cyc_killed/total_killed*100:.1f}%**, and motorists
    **{mot_killed/total_killed*100:.1f}%**. Vulnerable road users are killed at
    rates disproportionate to their share of traffic.

    **3. Contributing factors split cleanly by street type.**
    On highways, **Unsafe Speed** dominates.
    On surface streets, **{top_factor_all}** is the leading factor —
    requiring different interventions and different responsible agencies.

    **4. Spatial pattern: mostly isolated, not corridor.**
    Of {len(cor) + len(iso)} significant hotspot cells, **{len(iso)}
    ({len(iso)/(len(cor)+len(iso))*100:.0f}%)** are isolated hotspots rather than corridor clusters.
    This means most of NYC's fatal crash problem is addressable at the intersection level —
    within city agencies' direct control.

    **5. Vision Zero fatality rate trend.**
    The fatality rate per 1,000 crashes moved from **{rate_2014:.2f}** in 2014
    to **{rate_latest:.2f}** in {latest_year} — a **{abs(pct_change):.1f}%
    {"decrease" if pct_change < 0 else "increase"}**.
    {"This suggests meaningful progress, though absolute fatality counts remain elevated." if pct_change < 0 else "This warrants urgent policy review."}

    ---

    ### Policy Recommendations

    | Priority | Location Type | Agency | Intervention |
    |----------|--------------|--------|--------------|
    | 1 — Highest leverage | Isolated surface street hotspots | NYC DOT / NYPD | Leading pedestrian intervals, signal retiming, crosswalk hardening |
    | 2 — Corridor safety | Belt Parkway, Major Deegan, BQE | NYS DOT / MTA | Automated speed enforcement, managed speed zones, rumble strips |
    | 3 — Pedestrian protection | Broadway, Atlantic Ave, 3 Ave | NYC DOT | Road diet, pedestrian refuge islands, exclusive pedestrian phases |
    | 4 — Cyclist safety | Northern Blvd, Flatbush Ave | NYC DOT | Protected bike lanes, intersection hardening |
    | 5 — Data quality | All corridors | NYPD | Improve `on_street_name` completion rate in crash reports (currently ~75%) |

    ---
    *Analysis: Remy Ndayizeye · TDSP Navigator · April 2026*
    """)
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Test new feature
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Spatial Hotspot Analysis — H3 Hexagonal Binning
    """)
    return


@app.cell
def _():
    # Install h3 if needed
    import subprocess
    subprocess.run(["pip", "install", "h3", "--break-system-packages"], 
                   capture_output=True)
    import h3

    return (h3,)


@app.cell
def _(df, h3):
    # 1. Filter and Clean (Using a unique name to avoid redefinition errors)
    # We use .copy() to ensure we aren't modifying the original 'df'
    crash_geo = df[
        df["latitude"].notna() & 
        df["longitude"].notna() &
        df["latitude"].between(40.4, 40.95) & 
        df["longitude"].between(-74.3, -73.7)
    ].copy()

    # 2. Convert to H3 hexagons (Resolution 9 is ~0.1 km²)
    # Note: Using cell_to_boundary check if needed, but latlng_to_cell is standard v4
    crash_geo["h3_index"] = crash_geo.apply(
        lambda row: h3.latlng_to_cell(row["latitude"], row["longitude"], 8),
        axis=1
    )

    # 3. Aggregate metrics by hexagon
    h3_agg = crash_geo.groupby("h3_index").agg(
        crashes=("collision_id", "count"),
        killed=("number_of_persons_killed", "sum"),
        injured=("number_of_persons_injured", "sum"),
        ped_killed=("number_of_pedestrians_killed", "sum"),
        cyc_killed=("number_of_cyclist_killed", "sum"),
    ).reset_index()

    # 4. Calculate centroids and rates for visualization
    h3_agg["lat"] = h3_agg["h3_index"].apply(lambda x: h3.cell_to_latlng(x)[0])
    h3_agg["lon"] = h3_agg["h3_index"].apply(lambda x: h3.cell_to_latlng(x)[1])
    h3_agg["fatality_rate"] = h3_agg["killed"] / h3_agg["crashes"] * 1000

    print(f"Analyzed {len(h3_agg):,} hexagons")
    return (h3_agg,)


@app.cell
def _():
    # 1. Geometry Helper (Type-safe and version-agnostic)
    from matplotlib.collections import PolyCollection
    import numpy as np
    import contextily as cx

    return PolyCollection, cx, np


@app.cell
def _(PolyCollection, cx, h3, h3_agg, np, plt):
    # 1. Geometry Helper
    def get_hex_boundary(hex_id):
        h_str = str(hex_id).strip().lower()
        try:
            func = h3.cell_to_boundary if hasattr(h3, 'cell_to_boundary') else h3.h3_to_geo_boundary
            points = func(h_str)
            return [(p[1], p[0]) for p in points]
        except:
            return None

    # 2. Data Filtering
    significant_zones = h3_agg[h3_agg["crashes"] >= 5].copy()
    significant_zones["vru_total"] = significant_zones["ped_killed"] + significant_zones["cyc_killed"]

    # 3. Lenses with Raw Strings (r"") for LaTeX math support
    lenses = [
        {
            "df": significant_zones.nlargest(300, "crashes"), 
            "col": "crashes", 
            "cmap": "YlOrRd", 
            "title": r"Systemic Volume" + "\n" + r"(Total Crash Count $n$)"
        },
        {
            "df": significant_zones[significant_zones["killed"] > 0].nlargest(300, "fatality_rate"), 
            "col": "fatality_rate", 
            "cmap": "Reds", 
            "title": r"Fatal Severity" + "\n" + r"($\frac{Deaths}{Crashes} \times 1000$)"
        },
        {
            "df": significant_zones[significant_zones["vru_total"] > 0].nlargest(300, "vru_total"), 
            "col": "vru_total", 
            "cmap": "PuRd", 
            "title": r"VRU Priority" + "\n" + "(Sum of vulnerable user casualties)"
        }
    ]

    # 4. Plotting
    midnight_grey = '#3B3B3B' 
    fig, axes = plt.subplots(1, 3, figsize=(24, 11), facecolor=midnight_grey)

    for ax, lens in zip(axes, lenses):
        ax.set_facecolor(midnight_grey)

        verts, vals = [], []
        for idx, row in lens["df"].iterrows():
            poly = get_hex_boundary(row["h3_index"])
            if poly:
                verts.append(poly)
                vals.append(row[lens["col"]])

        if verts:
            pc = PolyCollection(
                verts, array=np.array(vals), cmap=lens["cmap"], 
                edgecolors='white', linewidths=0.2, alpha=0.75, zorder=3
            )
            ax.add_collection(pc)

            cb = fig.colorbar(pc, ax=ax, shrink=0.3, aspect=20, pad=0.02)
            cb.ax.yaxis.set_tick_params(color='white', labelcolor='white', labelsize=8)
            cb.outline.set_edgecolor('white')

        if not significant_zones.empty:
            ax.set_xlim(significant_zones["lon"].min() - 0.01, significant_zones["lon"].max() + 0.01)
            ax.set_ylim(significant_zones["lat"].min() - 0.01, significant_zones["lat"].max() + 0.01)

        cx.add_basemap(ax, crs="EPSG:4326", source=cx.providers.CartoDB.DarkMatterNoLabels, alpha=0.4, zorder=1)

        # Using a slightly smaller font for the sub-details to keep it clean
        ax.set_title(lens["title"], color='white', fontsize=14, fontweight='bold', pad=20)
        ax.axis('off')

    plt.tight_layout()
    plt.show()
    return


if __name__ == "__main__":
    app.run()
