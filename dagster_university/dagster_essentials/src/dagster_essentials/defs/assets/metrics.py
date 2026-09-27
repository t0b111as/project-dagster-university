import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd
from dagster_duckdb import DuckDBResource
import duckdb
import os
from dagster_essentials.defs.assets import constants

# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    # trips_by_zone = conn.execute(query).fetch_df()
    with database.get_connection() as conn:
        trips_by_zone=conn.execute(query).fetch_df()
    
    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim(-74.05, -73.90)  # Adjust longitude range
    ax.set_ylim(40.70, 40.82)  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


from datetime import datetime, timedelta
from dagster_essentials.defs.assets import constants

import pandas as pd
import dagster as dg
from dagster._utils.backoff import backoff
from dagster_essentials.defs import weekly_partition

# src/dagster_essentials/defs/assets/metrics.py
from dagster_essentials.defs.partitions import weekly_partition

@dg.asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The number of trips per week, aggregated by week.
    """

    period_to_fetch = context.partition_key

    # get all trips for the week
    query = f"""
        SELECT
            '{period_to_fetch}' AS period,
            COUNT(*) AS num_trips,
            COALESCE(SUM(total_amount), 0) AS total_amount,
            COALESCE(SUM(trip_distance), 0) AS trip_distance,
            COALESCE(SUM(passenger_count), 0) AS passenger_count
        FROM trips
        WHERE pickup_datetime >= '{period_to_fetch}'::date
        AND pickup_datetime < '{period_to_fetch}'::date + INTERVAL '1 week'
    """

    with database.get_connection() as conn:
        aggregate = conn.execute(query).fetch_df()

    # clean up the formatting of the dataframe
    aggregate["period"] = period_to_fetch
    aggregate['num_trips'] = aggregate['num_trips'].astype(int)
    aggregate['passenger_count'] = aggregate['passenger_count'].astype(int)
    aggregate['total_amount'] = aggregate['total_amount'].round(2).astype(float)
    aggregate['trip_distance'] = aggregate['trip_distance'].round(2).astype(float)
    aggregate = aggregate[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]

    try:
        # If the file already exists, append to it, but replace the existing month's data
        existing = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != period_to_fetch]
        existing = pd.concat([existing, aggregate]).sort_values(by="period")
        existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        aggregate.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)


    

