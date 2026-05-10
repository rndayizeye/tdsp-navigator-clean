import marimo

__generated_with = "0.23.2"
app = marimo.App(width="medium")


@app.cell
def _():
    #did you save?
    import marimo as mo
    from kedro.framework.session import KedroSession
    from kedro.framework.startup import bootstrap_project
    from pathlib import Path

    return KedroSession, Path, bootstrap_project


@app.cell
def _():
    import kedro
    print(kedro.__version__)
    return


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
def _(catalog):
    catalog.list()
    return


@app.cell
def _(catalog):
    nyc_crashes = catalog.load(name="nyc_crashes")
    return (nyc_crashes,)


@app.cell
def _(nyc_crashes):
    len(nyc_crashes)
    return


@app.cell
def _(catalog):
    nyc_census_data_geo = catalog.load("nyc_census_geodf")
    nyc_census_data_geo.head()
    return (nyc_census_data_geo,)


@app.cell
def _(nyc_census_data_geo):
    nyc_census_data_geo.plot()
    return


@app.cell
def _(catalog):
    nyc_census_data = catalog.load("nyc_census_raw")
    nyc_census_data.head()

    return


@app.cell
def _(catalog):
    nyc_census_data_geo_raw = catalog.load('nyc_census_geometry_raw')
    nyc_census_data_geo_raw.head()
    return


if __name__ == "__main__":
    app.run()
