from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Raw GHCN-D wide daily observations, one parquet file per year (1950-2026).
RAW_DAILY_PATH = (
    "/Volumes/global_daily_weather_analysis_catelog/weather_1950_2026_schema/"
    "raw_files_volumn/weather/data_wide"
)


@dp.table(
    name="bronze_daily_weather",
    comment=(
        "Bronze: raw GHCN-D wide daily weather observations (5,000 core stations, "
        "1950-2026, ~98M rows) ingested incrementally from the data_wide volume via Auto Loader."
    ),
    cluster_by=["station_id"],
)
@dp.expect("valid_observation_date", "date BETWEEN DATE'1950-01-01' AND DATE'2026-12-31'")
@dp.expect("known_station_id", "station_id IS NOT NULL")
def bronze_daily_weather():
    """Stream every raw daily-observation file as-is, tagging the ingest time.

    Auto Loader tracks processed files so new year files are picked up
    automatically on subsequent updates without reprocessing the archive.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(RAW_DAILY_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )
