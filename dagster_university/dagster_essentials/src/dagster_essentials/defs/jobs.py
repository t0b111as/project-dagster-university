import dagster as dg

#wichtig: asset hier erst auswählen -> 
# dg.asset_selection.assets("trips_by_week") 
# und dann in job selection verwenden, sonst 
# wird trips_by_week nicht ausgeführt!

trips_by_week = dg.AssetSelection.assets("trips_by_week")
adhoc_request = dg.AssetSelection.assets("adhoc_request")
from dagster_essentials.defs.partitions import monthly_partition, weekly_partition


trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition, # partitions added here
    selection=dg.AssetSelection.all() - trips_by_week - adhoc_request
)


trips_by_week_job = dg.define_asset_job(
    name="trips_by_week_job",
    selection=trips_by_week,
    partitions_def=weekly_partition, # partitions added here
    #alternativ: selection=dg.AssetSelection.assets("trips_by_week")
)

adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request,
)



