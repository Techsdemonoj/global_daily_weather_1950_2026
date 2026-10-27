from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Core-station dimension (5,000 stations) packaged with the archive.
RAW_STATIONS_PATH = (
    "/Volumes/global_daily_weather_analysis_catelog/weather_1950_2026_schema/"
    "raw_files_volumn/weather/agg/ghcnd_wide_stations.parquet"
)


@dp.materialized_view(
    name="bronze_stations",
    comment="Bronze: raw GHCN-D core station dimension (5,000 stations) loaded from the agg volume file.",
)
@dp.expect("known_station_id", "station_id IS NOT NULL")
def bronze_stations():
    """Load the station dimension exactly as packaged, tagging the ingest time."""
    return (
        spark.read.format("parquet")
        .load(RAW_STATIONS_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )
