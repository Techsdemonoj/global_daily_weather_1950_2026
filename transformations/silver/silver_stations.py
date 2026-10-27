from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _blank_to_null(col_name: str):
    """Trim a column and convert empty strings to NULL."""
    c = F.trim(F.col(col_name))
    return F.when(c != "", c)


@dp.materialized_view(
    name="silver_stations",
    comment=(
        "Silver: cleansed station dimension — identifiers trimmed, unknown-elevation "
        "sentinel (-999.9) set to NULL, blank flags/WMO IDs nulled."
    ),
)
@dp.expect_or_drop("valid_station_id", "station_id IS NOT NULL AND length(trim(station_id)) = 11")
@dp.expect("reasonable_coordinates", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")
def silver_stations():
    """Standardize the station dimension for joins and dashboards."""
    return spark.read.table("bronze_stations").select(
        F.trim("station_id").alias("station_id"),
        F.trim("station_name").alias("station_name"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.when(F.col("elevation") != -999.9, F.col("elevation"))
        .cast("double")
        .alias("elevation_m"),
        _blank_to_null("state").alias("state"),
        F.trim("country_code").alias("country_code"),
        F.trim("country_name").alias("country_name"),
        _blank_to_null("gsn_flag").alias("gsn_flag"),
        _blank_to_null("hcn_flag").alias("hcn_flag"),
        _blank_to_null("wmo_id").alias("wmo_id"),
        F.col("first_date").alias("first_obs_date"),
        F.col("last_date").alias("last_obs_date"),
        F.col("span_years").cast("int").alias("span_years"),
        F.col("span_days").cast("long").alias("span_days"),
    )
