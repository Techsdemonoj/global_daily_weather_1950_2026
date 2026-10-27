from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_country_climate_trends",
    comment=(
        "Gold: country-year climate trends — mean temperatures with anomalies against "
        "each country's 1951-1980 baseline, plus extreme-weather day counts."
    ),
    cluster_by=["country_name", "year"],
)
@dp.expect("stations_reported", "n_stations > 0")
def gold_country_climate_trends():
    """Business-ready warming-trend table: one row per country-year.

    Anomalies are computed against the 1951-1980 climatological baseline of
    each country's own station-year means. Aggregates use DECIMAL casts to
    keep the view incrementalizable.
    """
    daily = spark.read.table("silver_daily_weather")
    stations = spark.read.table("silver_stations").select("station_id", "country_name")

    observations = daily.join(stations, "station_id", "inner").where(
        F.col("country_name").isNotNull()
    )

    yearly = observations.groupBy("country_name", "year").agg(
        F.countDistinct("station_id").alias("n_stations"),
        F.round(F.avg(F.col("tmax_c").cast("decimal(7,2)")), 2).alias("avg_tmax_c"),
        F.round(F.avg(F.col("tmin_c").cast("decimal(7,2)")), 2).alias("avg_tmin_c"),
        F.round(
            F.avg((F.col("tmax_c") - F.col("tmin_c")).cast("decimal(7,2)")), 2
        ).alias("avg_diurnal_range_c"),
        F.round(
            F.avg(F.coalesce("prcp_mm", F.lit(0)).cast("decimal(10,2)")), 2
        ).alias("avg_daily_prcp_mm"),
        F.sum(F.when(F.col("tmax_c") >= 35, 1).otherwise(0)).alias("extreme_heat_days_35c"),
        F.sum(F.when(F.col("prcp_mm") >= 25.4, 1).otherwise(0)).alias("heavy_rain_days_1in"),
    )

    baseline = (
        yearly.where(F.col("year").between(1951, 1980))
        .groupBy("country_name")
        .agg(
            F.round(F.avg("avg_tmax_c"), 2).alias("baseline_avg_tmax_c"),
            F.round(F.avg("avg_tmin_c"), 2).alias("baseline_avg_tmin_c"),
        )
    )

    return (
        yearly.join(baseline, "country_name", "left")
        .select(
            "country_name",
            "year",
            "n_stations",
            "avg_tmax_c",
            "avg_tmin_c",
            "avg_diurnal_range_c",
            "avg_daily_prcp_mm",
            "extreme_heat_days_35c",
            "heavy_rain_days_1in",
            F.round(F.col("avg_tmax_c") - F.col("baseline_avg_tmax_c"), 2).alias(
                "tmax_anomaly_vs_1951_1980_c"
            ),
            F.round(F.col("avg_tmin_c") - F.col("baseline_avg_tmin_c"), 2).alias(
                "tmin_anomaly_vs_1951_1980_c"
            ),
        )
    )
