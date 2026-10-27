from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_station_year_summary",
    comment=(
        "Gold: station-year climate summary — mean/record temperatures, extremes "
        "and precipitation totals, enriched with station geography for maps and warming-trend analysis."
    ),
    cluster_by=["station_id", "year"],
)
@dp.expect("positive_day_count", "n_days > 0")
def gold_station_year_summary():
    """One row per station-year over the full 1950-2026 archive.

    Aggregates use DECIMAL casts so the view can refresh incrementally
    instead of rescanning ~98M daily rows on every update.
    """
    daily = spark.read.table("silver_daily_weather")
    stations = spark.read.table("silver_stations").select(
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "elevation_m",
        "state",
        "country_code",
        "country_name",
    )

    summary = daily.groupBy("station_id", "year").agg(
        F.count("*").alias("n_days"),
        F.round(F.avg(F.col("tmax_c").cast("decimal(7,2)")), 2).alias("tmax_mean_c"),
        F.round(F.avg(F.col("tmin_c").cast("decimal(7,2)")), 2).alias("tmin_mean_c"),
        F.round(
            F.avg((F.col("tmax_c") - F.col("tmin_c")).cast("decimal(7,2)")), 2
        ).alias("diurnal_range_mean_c"),
        F.max("tmax_c").alias("tmax_record_c"),
        F.min("tmin_c").alias("tmin_record_c"),
        F.round(
            F.sum(F.coalesce("prcp_mm", F.lit(0)).cast("decimal(10,2)")), 1
        ).alias("prcp_total_mm"),
        F.round(
            F.sum(F.coalesce("snow_mm", F.lit(0)).cast("decimal(10,2)")), 1
        ).alias("snow_total_mm"),
        F.sum(F.when(F.col("prcp_mm") >= 1.0, 1).otherwise(0)).alias("wet_days_1mm"),
        F.sum(F.when(F.col("prcp_mm") >= 25.4, 1).otherwise(0)).alias("heavy_rain_days_1in"),
        F.sum(F.when(F.col("tmax_c") >= 30, 1).otherwise(0)).alias("hot_days_30c"),
        F.sum(F.when(F.col("tmax_c") >= 35, 1).otherwise(0)).alias("extreme_heat_days_35c"),
        F.sum(F.when(F.col("tmin_c") < 0, 1).otherwise(0)).alias("frost_days"),
        F.sum(F.when(F.col("tmin_c") < -20, 1).otherwise(0)).alias("deep_freeze_days_m20c"),
        F.sum(F.when(F.col("snow_mm") > 0, 1).otherwise(0)).alias("snow_days"),
    )

    return summary.join(stations, "station_id", "left")
