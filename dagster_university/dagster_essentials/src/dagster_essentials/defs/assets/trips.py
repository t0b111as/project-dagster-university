import requests
from dagster_essentials.defs.assets import constants
import dagster as dg
import duckdb
import os
from dagster._utils.backoff import backoff
from dagster_duckdb import DuckDBResource
from dagster_essentials.defs.partitions import monthly_partition




@dg.asset(
        partitions_def=monthly_partition,
        group_name="raw_files"
)

def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal."""
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)


@dg.asset (
  description="The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.",
  group_name="raw_files"
)

def taxi_zones_file () -> None:
    """The raw taxi zones dataset. Sourced from the NYC Open Data portal."""
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)


@dg.asset(
  deps=["taxi_trips_file"],
  partitions_def=monthly_partition,
  group_name="ingested"
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
  """
    The raw taxi trips dataset, loaded into a DuckDB database, partitioned by month.
  """

  partition_date_str = context.partition_key
  month_to_fetch = partition_date_str[:-3]

    #falls trips noch nicht existiert -> leg Grundstruktur an!
  with database.get_connection() as conn:
      conn.execute("""
        create table if not exists trips (
          vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
          rate_code_id double, payment_type integer, dropoff_datetime timestamp,
          pickup_datetime timestamp, trip_distance double, passenger_count double,
          total_amount double, partition_date varchar
        )
      """)
      conn.execute("alter table trips add column if not exists partition_date varchar")
      conn.execute("""
        delete from trips
        where partition_date = ?
           or (
             partition_date is null
             and pickup_datetime >= ?::date
             and pickup_datetime < ?::date + interval '1 month'
           )
      """, [month_to_fetch, f"{month_to_fetch}-01", f"{month_to_fetch}-01"])
      conn.execute(f"""
        insert into trips (
          vendor_id, pickup_zone_id, dropoff_zone_id, rate_code_id, payment_type,
          dropoff_datetime, pickup_datetime, trip_distance, passenger_count,
          total_amount, partition_date
        )
        select
          VendorID, PULocationID, DOLocationID, RatecodeID, payment_type,
          tpep_dropoff_datetime, tpep_pickup_datetime, trip_distance,
          passenger_count, total_amount, ?
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}'
      """, [month_to_fetch])



@dg.asset(
    deps=["taxi_zones_file"],
    group_name="ingested"
)
def taxi_zones(database: DuckDBResource) -> None:
    """
    The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """
    with database.get_connection() as conn:
        conn.execute(query)