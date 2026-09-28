import dagster as dg


raw_assets_job = dg.define_asset_job(
    name="raw_assets_job",
    selection=dg.AssetSelection.groups("raw"),
)
